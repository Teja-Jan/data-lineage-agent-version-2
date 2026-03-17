import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from src.agent.llm_agent import run_mock_agent

def test_lineage_prompt():
    prompt = "I want to get lineage for a specific table. i ned to get all information on fact table. with a pictorial representation too."
    print("Testing Lineage Prompt:")
    print("-----------------------")
    response = run_mock_agent(prompt)
    print(response)
    print("\n")

def test_impact_prompt():
    prompt = "What impact i'll have if i go and drop the column customer_id"
    print("Testing Impact Prompt:")
    print("----------------------")
    response = run_mock_agent(prompt)
    print(response)
    print("\n")

def test_audit_prompt():
    prompt = "Who are all having access to the table, who has access to this specific ETL pipeline, when the ETL pipline has ran previously"
    print("Testing Audit Prompt:")
    print("---------------------")
    response = run_mock_agent(prompt)
    print(response)
    print("\n")

if __name__ == "__main__":
    test_lineage_prompt()
    test_impact_prompt()
    test_audit_prompt()
    print("All tests executed.")
