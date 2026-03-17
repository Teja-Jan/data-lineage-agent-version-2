import os
import sys

# Add parent directory to path to import src modules
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from src.agent.llm_agent import run_mock_agent

def test_unknown_query():
    print("Testing Unknown Query:")
    print("----------------------")
    prompt = "What is the weather in New York?"
    response = run_mock_agent(prompt)
    print(f"Response: {response}")
    assert "I'm sorry" in response
    assert "data catalog" in response
    print("SUCCESS: Hallucination prevented.\n")

def test_missing_table_lineage():
    from src.agent.agent_tools import get_table_lineage
    print("Testing Missing Table Lineage:")
    print("------------------------------")
    response = get_table_lineage.run("NonExistentTable")
    print(f"Response: {response}")
    assert "DATA NOT FOUND" in response
    print("SUCCESS: Tool grounding verified.\n")

if __name__ == "__main__":
    test_unknown_query()
    test_missing_table_lineage()
    print("All hallucination prevention tests passed.")
