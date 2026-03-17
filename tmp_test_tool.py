import sys
import os
sys.path.append(os.path.abspath('src'))
from src.agent.agent_tools_ext import generate_e2e_lineage_graph

try:
    print("Calling tool.run() for a Report...")
    # 'Monthly Revenue Trend' or similar is a BI report in the DB
    res = generate_e2e_lineage_graph.run("Monthly Revenue Trend")
    print(f"Result: {res}")
except Exception as e:
    print(f"Exception: {type(e).__name__}: {e}")


