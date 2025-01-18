from typing import Iterable
import ifcopenshell
from neo4j import GraphDatabase
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def connect_to_neo4j(uri, user, password):
    """Connect to the Neo4j instance."""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

def create_if_not_exists(driver, database_name):
    """Create a new database in Neo4j if it does not already exist."""
    with driver.session(database="system") as session:
        result = session.run("SHOW DATABASES")
        existing_databases = [record["name"] for record in result]

        if database_name in existing_databases:
            print(f"Database '{database_name}' already exists. Skipping creation.")
        else:
            session.run(f"CREATE DATABASE {database_name}")
            print(f"Database '{database_name}' created successfully.")

def create_nodes_in_batch(driver, batch, file_id, database):
    """Create a batch of nodes in Neo4j."""
    def create_node(tx, entities):
        for entity in entities:
            entity_type = entity.is_a()
            entity_id = entity.id()
            attributes = entity.get_info()
            attributes["file_id"] = file_id
            attributes["entity_id"] = entity_id

            # Filter scalar attributes
            def is_neo4j_compatible(value):
                return isinstance(value, (str, int, float, bool))

            scalar_attributes = {k: v for k, v in attributes.items() if is_neo4j_compatible(v)}

            # Use MERGE to avoid duplicates
            cypher_query = f"""
            MERGE (n:{entity_type} {{ entity_id: $entity_id, file_id: $file_id }})
            SET {", ".join([f"n.{k} = ${k}" for k in scalar_attributes.keys()])}
            """
            tx.run(cypher_query, **scalar_attributes)

    with driver.session(database=database) as session:
        session.execute_write(create_node, batch)

def create_relationships_in_batch(driver, batch, file_id, database):
    """Create relationships in batches."""
    def create_relationship(tx, entities):
        # Define a single Cypher query to handle relationships
        cypher_query = """
        MATCH (a {{entity_id: $start_id, file_id: $file_id}})
        MATCH (b {{entity_id: $end_id, file_id: $file_id}})
        MERGE (a)-[:{rel_name}]->(b)
        MERGE (b)-[:REVERSE_{rel_name}]->(a)
        """

        for entity in entities:
            for rel_name, rel_value in entity.get_info().items():
                if isinstance(rel_value, ifcopenshell.entity_instance):
                    # Handle single Ifc entity relationships
                    tx.run(
                        cypher_query.format(rel_name=rel_name),
                        start_id=entity.id(),
                        end_id=rel_value.id(),
                        file_id=file_id,
                    )
                elif isinstance(rel_value, Iterable) and all(isinstance(item, ifcopenshell.entity_instance) for item in rel_value):
                    # Handle iterable relationships
                    for related_entity in rel_value:
                        tx.run(
                            cypher_query.format(rel_name=rel_name),
                            start_id=entity.id(),
                            end_id=related_entity.id(),
                            file_id=file_id,
                        )

    with driver.session(database=database) as session:
        session.execute_write(create_relationship, batch)

def parse_ifc_and_populate_neo4j(ifc_file_path, driver, database, file_id):
    """Parse IFC file and populate Neo4j."""
    ifc_file = ifcopenshell.open(ifc_file_path)

    entities = sorted(ifc_file, key=lambda e: e.id())
    total_entities = len(entities)
    print(f"Total entities to process: {total_entities}")

    # Batch size for parallel processing
    batch_size = 500
    batches = [entities[i:i + batch_size] for i in range(0, total_entities, batch_size)]

    # Process Nodes
    print("Processing nodes...")
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_nodes_in_batch, driver, batch, file_id, database) for batch in batches]
        for i, future in enumerate(as_completed(futures), 1):
            future.result()  # Wait for batch to complete
            print(f"Processed batch {i}/{len(batches)} (Nodes)")
    node_time = time.time() - start_time
    print(f"Node creation completed in {node_time:.2f} seconds.")

    batches = [entities]

    # Process Relationships
    print("Processing relationships...")
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_relationships_in_batch, driver, batch, file_id, database) for batch in batches]
        for i, future in enumerate(as_completed(futures), 1):
            future.result()  # Wait for batch to complete
            print(f"Processed batch {i}/{len(batches)} (Relationships)")
    relationship_time = time.time() - start_time
    print(f"Relationship creation completed in {relationship_time:.2f} seconds.")

    # Total Time
    total_time = node_time + relationship_time
    print(f"Total processing time: {total_time:.2f} seconds.")

def main():
    # Hardcoded IFC file path
    ifc_file_path = r"C:\Users\Public\Solibri\SOLIBRI\Samples\ifc\Solibri Building.ifc"
    ifc_file_path = r"C:\Users\Public\Solibri\SOLIBRI\Samples\ifc\Solibri Building Structural.ifc"

    # Extract and sanitize the database name
    database_name = os.path.splitext(os.path.basename(ifc_file_path))[0]
    database_name = re.sub(r"[^A-Za-z0-9.]", ".", database_name).lower().strip(".")

    # Configuration
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    file_id = database_name

    # Connect to Neo4j
    driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    # Create the database if it does not exist
    create_if_not_exists(driver, database_name)

    # Parse IFC file and populate the database
    parse_ifc_and_populate_neo4j(ifc_file_path, driver, database_name, file_id)

    # Close the connection
    driver.close()
    print(f"Finished populating the database '{database_name}'.")

if __name__ == "__main__":
    main()
