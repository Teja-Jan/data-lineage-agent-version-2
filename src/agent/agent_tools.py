import sqlite3
import os
import networkx as nx
from pyvis.network import Network
from langchain.tools import tool

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_db_connection():
    return sqlite3.connect(TARGET_DB_PATH)

@tool
def get_table_details(table_name: str) -> str:
    """Returns the schema and description of a table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    conn.close()
    
    if not columns:
        return f"TABLE NOT FOUND: {table_name}"
        
    report = f"### Schema Details for {table_name}:\n"
    for col in columns:
        report += f" - `{col[1]}` ({col[2]})\n"
    return report

@tool
def get_table_lineage(table_name: str) -> str:
    """Finds the data lineage for a specific table. Use this for finding where data comes from (Upstream) and goes to (Downstream)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Column level lineage
    cursor.execute("SELECT source_system, source_table, source_column, target_column, transformation_rule FROM data_lineage_map WHERE target_table = ?", (table_name,))
    upstream = cursor.fetchall()
    
    cursor.execute("SELECT target_table, target_column, source_column, transformation_rule FROM data_lineage_map WHERE source_table = ?", (table_name,))
    downstream = cursor.fetchall()
    
    conn.close()
    
    report = f"### Data Lineage Intelligence Report: {table_name}\n"
    report += f"**Metadata Status:** Synchronized | **Catalog Source:** Enterprise DW\n"
    
    if upstream:
        report += "\n#### UPSTREAM SOURCES (Ingestion Flow)\n"
        # Group by source table for cleaner report
        sources = {}
        for row in upstream:
            key = f"{row[0]}: {row[1]}"
            if key not in sources: sources[key] = []
            sources[key].append(f"`{row[2]}` -> `{row[3]}` ({row[4]})")
        
        for src, cols in sources.items():
            report += f"- **{src}**\n  - " + "\n  - ".join(cols) + "\n"
            
    if downstream:
        report += "\n#### DOWNSTREAM TARGETS (Distribution Flow)\n"
        targets = {}
        for row in downstream:
            key = row[0]
            if key not in targets: targets[key] = []
            targets[key].append(f"`{row[2]}` -> `{row[1]}` ({row[3]})")
            
        for tgt, cols in targets.items():
            report += f"- **{tgt}**\n  - " + "\n  - ".join(cols) + "\n"
            
    if not upstream and not downstream:
        return f"DATA NOT FOUND: No lineage information exists for table '{table_name}' in the current metadata catalog."
        
    return report

@tool
def generate_lineage_graph(table_name: str) -> str:
    """Generates a pictorial representation (HTML graph) of the data lineage for a table. Returns the path to the HTML file."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT source_table, target_table FROM data_lineage_map")
    all_edges = set(cursor.fetchall())
    conn.close()
    
    G = nx.DiGraph()
    for src, tgt in all_edges:
        G.add_edge(src, tgt)
        
    if table_name not in G:
        return f"VISUALIZATION ERROR: Table '{table_name}' not found in the lineage mapping graph."
        
    ancestors = nx.ancestors(G, table_name)
    descendants = nx.descendants(G, table_name)
    nodes_of_interest = ancestors.union(descendants).union({table_name})
    sub_g = G.subgraph(nodes_of_interest)
    
    # Modern Visualization (Light Theme)
    net = Network(height="600px", width="100%", bgcolor="#FFFFFF", font_color="#0F172A", directed=True)
    
    for node in sub_g.nodes:
        color = "#2563EB" # Blue for target
        if node == table_name: color = "#EF4444" # Red for selected
        elif node in ancestors: color = "#10B981" # Green for source
        elif node in descendants: color = "#F59E0B" # Orange for downstream
        
        net.add_node(node, label=node, color=color, shape="ellipse")
            
    for source, target in sub_g.edges:
        net.add_edge(source, target, color="#94A3B8")
        
    import json
    net.set_options(json.dumps({
        "physics": {
            "enabled": False
        },
        "layout": {
            "hierarchical": {
                "enabled": True,
                "direction": "LR",
                "sortMethod": "directed",
                "levelSeparation": 180,
                "nodeSpacing": 120
            }
        },
        "interaction": {
            "navigationButtons": True,
            "keyboard": True,
            "zoomView": True
        }
    }))
        
    output_path = os.path.join(OUTPUT_DIR, f"{table_name}_lineage.html")
    net.save_graph(output_path)
    return f"Pictorial representation generated successfully. View it at: {output_path}"

@tool
def get_column_impact(table_name: str, column_name: str, change_type: str = "drop") -> str:
    """Analyzes the impact of dropping, renaming, or changing the datatype of a specific column.
    
    Arguments:
    - table_name: The table where the column resides.
    - column_name: The column being modified.
    - change_type: One of 'drop', 'rename', 'datatype'.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT target_table, target_column, transformation_rule FROM data_lineage_map WHERE source_table = ? AND source_column = ?", (table_name, column_name))
    downstream = cursor.fetchall()
    
    # Get current datatype
    cursor.execute(f"PRAGMA table_info({table_name})")
    col_info = [c for c in cursor.fetchall() if c[1] == column_name]
    current_type = col_info[0][2] if col_info else "UNKNOWN"
    
    conn.close()
    
    report = f"### [PREDICTIVE] Impact Analysis: {table_name}.{column_name}\n"
    report += f"**Action Type:** {change_type.upper()} | **Current Type:** `{current_type}`\n\n"

    if not downstream:
        report += f"✅ **LOW RISK:** No direct downstream dependencies found in the lineage map. However, ad-hoc queries may still be affected.\n"
        if change_type == "datatype":
            report += "💡 *Note: Changing datatype may still affect indexing and storage performance.*\n"
    else:
        status = "CRITICAL" if change_type in ["drop", "rename"] else "MODERATE"
        report += f"⚠️ **{status} RISK:** This field has {len(downstream)} mapped downstream dependencies.\n"
        
        for row in downstream:
            risk_desc = "Breakage" if change_type != "datatype" else "Precision Loss / Casting failure"
            report += f" - **{row[0]}**: `{row[1]}` ({row[2]}) -> **Risk:** {risk_desc}\n"
        
        if change_type == "datatype":
            report += "\n**Prediction:** Downstream transformation rules involving numeric calculations may fail if precision is lost (e.g., FLOAT -> INT)."
        else:
            report += "\n**Prediction:** All downstream pipelines and reporting views utilizing this column will CRASH immediately."

    return report

@tool
def get_metadata_inventory() -> str:
    """Returns a full inventory of all tables and columns in the enterprise data warehouse for discovery."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [t[0] for t in cursor.fetchall()]
    
    inventory = "### 🗄️ Enterprise Data Catalog: Metadata Inventory\n"
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        col_list = ", ".join([f"`{c[1]}` ({c[2]})" for c in cols])
        inventory += f"- **{table}**: {col_list}\n"
        
    conn.close()
    return inventory

@tool
def get_pipeline_history(pipeline_name: str) -> str:
    """Gets the execution history and audit changes for an ETL pipeline. Covers historical record counts and success trends."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Last 10 executions for trend
    cursor.execute("SELECT start_time, end_time, records_inserted, status, transformation_metrics FROM etl_execution_logs WHERE pipeline_name = ? ORDER BY start_time DESC LIMIT 10", (pipeline_name,))
    executions = cursor.fetchall()
    
    # Audit changes
    cursor.execute("SELECT modification_time, modified_by, version_tag, change_summary FROM etl_pipeline_audit WHERE pipeline_name = ? ORDER BY modification_time DESC", (pipeline_name,))
    audits = cursor.fetchall()
    
    conn.close()
    
    report = f"### Pipeline Insight: {pipeline_name}\n"
    if executions:
        report += "\n#### Recent Execution Trends:\n"
        for row in executions:
            metrics = json.loads(row[4]) if row[4] else {}
            revenue = metrics.get('total_revenue', 'N/A')
            report += f" - {row[0][:16]} | Status: **{row[3]}** | Loaded: `{row[2]}` records | Context: `{revenue}`\n"
            
    if audits:
        report += "\n#### Audit Log / Deployment History:\n"
        for row in audits:
            report += f" - [{row[0][:10]}] **v{row[2]}** by {row[1]}: {row[3]}\n"
            
    if not executions and not audits:
        return f"DATA NOT FOUND: Pipeline '{pipeline_name}' has no registered execution history."
        
    return report

@tool
def get_table_access(table_name: str) -> str:
    """Checks the Database Audit Log for access history and permission changes on a table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT event_time, event_type, changed_by_user, change_description, impact_score FROM db_audit_log WHERE target_object = ? ORDER BY event_time DESC", (table_name,))
    logs = cursor.fetchall()
    conn.close()
    
    if not logs:
        return f"ACCESS DATA NOT FOUND: No audit events for table '{table_name}'."
        
    report = f"### Access & Audit History for {table_name}:\n"
    for row in logs:
        score_marker = "🔴" if row[4] > 70 else "🟡" if row[4] > 30 else "🟢"
        report += f" - {score_marker} [{row[0][:16]}] **{row[1]}** by `{row[2]}`: {row[3]}\n"
    return report

@tool
def get_schema_evolution() -> str:
    """Provides a report on recent schema modifications across the whole database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT event_time, target_object, changed_by_user, change_description FROM db_audit_log WHERE event_type = 'DDL' ORDER BY event_time DESC LIMIT 10")
    logs = cursor.fetchall()
    conn.close()
    
    if not logs:
        return "No recent schema changes found."
        
    report = "### Database Schema Evolution (Last 10 DDL Events):\n"
    for row in logs:
        report += f" - **{row[1]}** changed on {row[0][:16]} by {row[2]}: {row[3]}\n"
    return report

