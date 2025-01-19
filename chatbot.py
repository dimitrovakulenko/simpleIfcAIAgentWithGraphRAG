from typing import Literal
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openai import AzureChatOpenAI
from langgraph.graph import END, START, StateGraph, MessagesState
from langgraph.prebuilt import ToolNode
from neo4j import Driver

class Chatbot:
    def __init__(self):
        self.driver = None
        self.database = None
        self.agent = None
        self.messages = []

    def initialize(self, driver: Driver, database: str):
        self.driver = driver
        self.database = database

        if not self.driver or not self.database:
            raise ValueError("Driver and database must be provided for initialization.")

        self.agent = get_or_create_agent(self)

    async def message(self, user_message: str) -> str:
        if not self.agent:
            return "Chatbot is not initialized."

        try:
            if len(self.messages) == 0:
                self.messages = [HumanMessage(user_message)]
            else:
                self.messages.append(HumanMessage(user_message))

            result = await self.agent.ainvoke({"messages": self.messages},
                                config={
                                    "recursion_limit": 42,
                                    "configurable": {"thread_id": "42"}})
            
            self.messages = result["messages"]
            
            response_message = result["messages"][-1]

            return response_message.content

        except Exception as e:
            print(f"Error: {str(e)}")
            return f"Error: {str(e)}"

# Tools for the LLM to prepare an answer

def create_query_ifc_graph_database(chatbot: Chatbot):
    @tool
    async def query_ifc_graph_database(cypher_query):
        """Executes cypher_query in neo4j graph database storing ifc file."""
        print(f'Executes query: {cypher_query}')

        try:
            def create_tx(tx):
                result = tx.run(cypher_query)
                return [record.data() for record in result]
            
            with chatbot.driver.session(database=chatbot.database) as session:
                results = session.execute_read(create_tx)

            return results
        except Exception as e:
            print(f'Error: {str(e)}')            

    return query_ifc_graph_database

# LLM configuration

def create_call_model(llm_client, chatbot: Chatbot, max_tokens=128000, buffer_tokens=500):
    async def call_model(state: MessagesState):
        print('LLM generates answer...')
        messages = state['messages']

        if len(messages) == 1:
            messages = [
                SystemMessage(
                    "You are a chabot assistant answering questions about specific ifc file."
                    "You translate user questions to ifc terminology."
                    "You access ifc file via neo4j graph database."
                    "You have full access to that database and can execute any query on it using query_ifc_graph_database tool."
                    "If you didn't recieve the answer from the first try you can make more attempts with other queries."
                    "You can call that tool up to 20 times during the preparation of one answer."
                    "In the database all ifc entities of the ifc file correspond nodes, ifc entities attributes are nodes attributes."
                    "When ifc entity references another ifc entity - this is a link/relationship in the neo4j database."
                    "For example a node of type IfcAxis2Placement3D has a relationship 'Axis' to node of type IfcDirection."
                    "Database doesn't change during the chat session."
                    "Before making a query that requires a specific label, you can first execute a query that will check if that label exists."
                ),
                messages[0]
            ]
        
        response = await llm_client.ainvoke(messages)

        print(f"Total tokens usage: {response.usage_metadata.get('total_tokens')}")

        messages.append(response)

        state['messages'] = messages

        return {"messages": [response]}
    return call_model

def should_continue(state: MessagesState) -> Literal["tools", END]:
    messages = state['messages']
    last_message = messages[-1]

    if last_message.tool_calls:
        return "tools"

    return END

# Assemble agent

def get_or_create_agent(chatbot):
    query_ifc_graph_database = create_query_ifc_graph_database(chatbot)

    tools = [
        query_ifc_graph_database
    ]

    llm_client = AzureChatOpenAI(
        deployment_name="gpt-4o-mini", 
        temperature=0.3, 
        max_tokens=4000
    ).bind_tools(tools)
    
    call_model = create_call_model(llm_client, chatbot)

    workflow = StateGraph(MessagesState)
    workflow.add_node("agent", call_model)
    workflow.add_node("tools", ToolNode(tools))
    workflow.add_edge(START, "agent")
    workflow.add_conditional_edges("agent", should_continue)
    workflow.add_edge("tools", "agent")

    return workflow.compile()
