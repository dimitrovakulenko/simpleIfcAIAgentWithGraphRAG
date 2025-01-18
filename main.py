from ifc_to_neo4j import process_ifc_file

if __name__ == "__main__":
    ifc_file_path = r"C:\Users\Public\Solibri\SOLIBRI\Samples\ifc\Solibri Building.ifc"
    ifc_file_path = r"C:\Users\Public\Solibri\SOLIBRI\Samples\ifc\Solibri Building Structural.ifc"

    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"

    process_ifc_file(ifc_file_path, neo4j_uri, neo4j_user, neo4j_password)
