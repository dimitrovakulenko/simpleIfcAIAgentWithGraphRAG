from typing import Iterable
import ifcopenshell
from neo4j import Driver
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

PROCESSING_BATCH_SIZE = 500

def does_database_exist(driver, database_name):
    with driver.session(database="system") as session:
        result = session.run("SHOW DATABASES")
        existing_databases = [record["name"] for record in result]
        return database_name in existing_databases

def create_database(driver, database_name):   
    with driver.session(database="system") as session:
        session.run(f"CREATE DATABASE {database_name}")
        print(f"Database '{database_name}' created successfully.")

def clean_database(driver, database_name):
    with driver.session(database=database_name) as session:
        session.run("MATCH (n) DETACH DELETE n")
    print(f"Database '{database_name}' has been cleaned.")

def create_nodes_in_batch(driver, batch, database):
    def create_node(tx, entities):
        for entity in entities:
            entity_type = entity.is_a()
            entity_id = entity.id()
            attributes = entity.get_info()
            attributes["entity_id"] = entity_id

            def is_neo4j_compatible(value):
                return isinstance(value, (str, int, float, bool))

            scalar_attributes = {k: v for k, v in attributes.items() if is_neo4j_compatible(v)}

            cypher_query = f"""
            MERGE (n:{entity_type} {{ entity_id: $entity_id }})
            SET {", ".join([f"n.{k} = ${k}" for k in scalar_attributes.keys()])}
            """
            tx.run(cypher_query, **scalar_attributes)

    with driver.session(database=database) as session:
        session.execute_write(create_node, batch)

def create_relationships_in_batch(driver, batch, database):
    def create_relationship(tx, entities):
        cypher_query = """
        MATCH (a {{entity_id: $start_id}})
        MATCH (b {{entity_id: $end_id}})
        MERGE (a)-[:{rel_name}]->(b)
        MERGE (b)-[:REVERSE_{rel_name}]->(a)
        """

        counter = 0

        for entity in entities:
            counter += 1
            for rel_name, rel_value in entity.get_info().items():
                if isinstance(rel_value, ifcopenshell.entity_instance):
                    tx.run(
                        cypher_query.format(rel_name=rel_name),
                        start_id=entity.id(),
                        end_id=rel_value.id()
                    )
                elif isinstance(rel_value, Iterable):
                    if all(isinstance(item, ifcopenshell.entity_instance) for item in rel_value):
                        for related_entity in rel_value:
                            tx.run(
                                cypher_query.format(rel_name=rel_name),
                                start_id=entity.id(),
                                end_id=related_entity.id()
                            )

            if counter % PROCESSING_BATCH_SIZE == 0:
                print(f"Processed {counter} nodes")

    with driver.session(database=database) as session:
        session.execute_write(create_relationship, batch)

def parse_ifc_and_populate_neo4j(ifc_file_path, driver, database):
    ifc_file = ifcopenshell.open(ifc_file_path)

    entities = sorted(ifc_file, key=lambda e: e.id())
    total_entities = len(entities)
    print(f"Total entities to process: {total_entities}")

    batch_size = PROCESSING_BATCH_SIZE 
    batches = [entities[i:i + batch_size] for i in range(0, total_entities, batch_size)]

    print("Processing nodes...")
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_nodes_in_batch, driver, batch, database) for batch in batches]
        for i, future in enumerate(as_completed(futures), 1):
            future.result()  # Wait for batch to complete
            print(f"Processed batch {i}/{len(batches)} (Nodes)")
    node_time = time.time() - start_time
    print(f"Node creation completed in {node_time:.2f} seconds.")

    print("Processing relationships...")
    # TODO: come up with efficient batching, for now one batch
    batches = [entities] 
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_relationships_in_batch, driver, batch, database) for batch in batches]
        for i, future in enumerate(as_completed(futures), 1):
            future.result()
            print(f"Processed batch {i}/{len(batches)} (Relationships)")
    relationship_time = time.time() - start_time
    print(f"Relationship creation completed in {relationship_time:.2f} seconds.")

    total_time = node_time + relationship_time
    print(f"Total processing time: {total_time:.2f} seconds.")

def process_ifc_file(ifc_file_path, driver:Driver, db_name=None, clean_db=True):
    """
    Process an IFC file and populate a Neo4j database with the extracted data.
    All IFC entities will become nodes, all links between ifc entities will become links between nodes.

    Args:
        ifc_file_path (str): Path to the IFC file to process.
        neo4j_uri (str): URI of the Neo4j instance (e.g., "bolt://localhost:7687").
        neo4j_user (str): Username for Neo4j authentication.
        neo4j_password (str): Password for Neo4j authentication.
        db_name (str, optional): Name of the Neo4j database to use. Defaults to a name derived from the IFC file.
        clean_db (bool, optional): Whether to clean the database before populating it
    """

    database_name = db_name or os.path.splitext(os.path.basename(ifc_file_path))[0]
    database_name = re.sub(r"[^A-Za-z0-9.]", ".", database_name).lower().strip(".")

    try:
        if does_database_exist(driver, database_name):
            print(f"Database '{database_name}' already exists. Skipping creation.")
            if clean_db:
                clean_database(driver, database_name)
        else:
            create_database(driver, database_name)

        parse_ifc_and_populate_neo4j(ifc_file_path, driver, database_name)

        print(f"Finished populating the database '{database_name}'.")
    finally:
        driver.close()
