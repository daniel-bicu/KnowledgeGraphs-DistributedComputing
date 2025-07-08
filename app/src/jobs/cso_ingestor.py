import json
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from app.src.jobs.cso_parser import generate_cso_canonical_hierarchy
file_path = "../../data/cso_hierarchy_canonical.json"

load_dotenv()
neo4j_url = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_pwd = os.getenv("NEO4J_PASSWORD")
neo4j_db = os.getenv("NEO4J_PASSWORD")
driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_pwd))

def load_cso_hierarchy(json_path):
    with open(json_path, "r") as f:
        return json.load(f)

with open("cso_hierarchy_canonical.json", "r") as f:
    data = json.load(f)

def create_topics_and_relationships(session, data):
    for parent, children in data.items():
        for child in children:
            session.run("""
                MERGE (p:Topic {name: $parent})
                MERGE (c:Topic {name: $child})
                MERGE (c)-[:SUBTOPIC_OF]->(p)
            """, parent=parent, child=child)


with driver.session() as session:
    if os.path.exists(file_path):
        print("File exists. Using cso hierarchy canonical.")
        cso_data = load_cso_hierarchy("cso_hierarchy_canonical.json")
    else:
        print("File is not generated.")
        print("Generate & Load")
        cso_data = load_cso_hierarchy(generate_cso_canonical_hierarchy('../../data/CSO.3.4.csv'))

    create_topics_and_relationships(session, cso_data)
    print("Topics and relationships loaded into Neo4j.")
