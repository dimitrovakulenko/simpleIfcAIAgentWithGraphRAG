import ifcopenshell
from neo4j import GraphDatabase
import os
import re

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

def create_node(tx, entity, file_id):
    """Create a node in Neo4j."""
    entity_type = entity.is_a()
    entity_id = entity.id()
    attributes = entity.get_info()
    attributes["file_id"] = file_id
    attributes["entity_id"] = entity_id

    # Filter and transform scalar attributes to ensure compatibility with Neo4j
    def is_neo4j_compatible(value):
        return isinstance(value, (str, int, float, bool))  # Allow only Neo4j-compatible types

    scalar_attributes = {k: v for k, v in attributes.items() if is_neo4j_compatible(v)}

    # Create the node
    cypher_query = f"""
    CREATE (n:{entity_type} {{ {", ".join([f"{k}: ${k}" for k in scalar_attributes.keys()])} }})
    """
    tx.run(cypher_query, **scalar_attributes)

def create_relationship(tx, start_id, end_id, relationship_type, file_id):
    """Create a relationship between two nodes in Neo4j."""
    cypher_query = """
    MATCH (a {entity_id: $start_id, file_id: $file_id})
    MATCH (b {entity_id: $end_id, file_id: $file_id})
    CREATE (a)-[:{relationship_type}]->(b)
    """
    tx.run(cypher_query, start_id=start_id, end_id=end_id, relationship_type=relationship_type, file_id=file_id)

def parse_ifc_and_populate_neo4j(ifc_file_path, driver, database, file_id):
    """Parse IFC file and populate Neo4j."""
    ifc_file = ifcopenshell.open(ifc_file_path)

    with driver.session(database=database) as session:
        for entity in ifc_file:
            session.write_transaction(create_node, entity, file_id)

        # Second Pass: Add Relationships
        for entity in ifc_file:
            for rel_name, rel_value in entity.get_info().items():
                if isinstance(rel_value, ifcopenshell.entity_instance):
                    session.write_transaction(
                        create_relationship, entity.id(), rel_value.id(), rel_name, file_id
                    )
                elif isinstance(rel_value, list):
                    for related_entity in rel_value:
                        session.write_transaction(
                            create_relationship, entity.id(), related_entity.id(), rel_name, file_id
                        )

def main():
    ifc_file_path = r"C:\Users\Public\Solibri\SOLIBRI\Samples\ifc\Solibri Building.ifc"

    database_name = os.path.splitext(os.path.basename(ifc_file_path))[0]
    database_name = re.sub(r"[^A-Za-z0-9.]", ".", database_name).lower().strip(".")  

    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    file_id = database_name

    driver = connect_to_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    create_if_not_exists(driver, database_name)

    parse_ifc_and_populate_neo4j(ifc_file_path, driver, database_name, file_id)

    driver.close()
    print(f"Finished populating the database '{database_name}'.")

if __name__ == "__main__":
    main()
