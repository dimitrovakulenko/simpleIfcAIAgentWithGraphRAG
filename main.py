import asyncio
from neo4j import GraphDatabase
from chatbot import Chatbot
from ifc_to_neo4j import process_ifc_file

def connect_to_neo4j(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

async def main():
    ifc_file_path = r"C:\Users\Public\Solibri\SOLIBRI\Samples\ifc\Solibri Building Structural.ifc"

    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"

    database_name = "test2.db"

    fill_db = True

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    if fill_db:
        process_ifc_file(ifc_file_path, driver, database_name)

    bot = Chatbot()
    bot.initialize(driver, database_name)

    while True:
        user_input = input("\nYou: ")
        
        if user_input.lower() in {"exit", "quit", "bye"}:
            print("Exiting chat. Goodbye!")
            break
        
        try:
            bot_response = await bot.message(user_input)
            print(f"\nBot: {bot_response}")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())