"""
Enterprise Extension Tools for the AI Data Lineage Agent.
These tools provide data model documentation, comprehensive impact analysis,
and 3-tier end-to-end lineage visualization.
"""
import json
import os
import sqlite3
import networkx as nx
from pyvis.network import Network
import pandas as pd
from langchain.tools import tool
try:
    import chromadb
except ImportError:
    chromadb = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')
OUTPUT_DIR = os.path.join(BASE_DIR, 'reports', 'visualizations')
CHROMA_DB_DIR = os.path.join(BASE_DIR, 'data', 'chroma_db')

os.makedirs(OUTPUT_DIR, exist_ok=True)

def _get_conn():
    return sqlite3.connect(DB_PATH)


@tool
def get_data_model_description(table_name: str) -> str:
    """Returns a detailed data model description for a Fact or Dimension table.
    Includes business purpose, type, source system, ETL pipeline, linked tables,
    key measures and derivations explained in plain English, and column-level schema with lineage."""
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM table_catalog WHERE table_name = ?", (table_name,))
    catalog = cursor.fetchone()

    cursor.execute(f'PRAGMA table_info("{table_name}")')

    columns = cursor.fetchall()

    cursor.execute("""
        SELECT source_system, source_table, source_column, target_column, transformation_rule
        FROM data_lineage_map WHERE target_table = ?
    """, (table_name,))
    lineage_rows = cursor.fetchall()
    conn.close()

    if not catalog and not columns:
        return f"TABLE NOT FOUND: '{table_name}' is not registered in the data catalog."

    # Human-readable type overrides for common SQLite blank/generic types
    type_labels = {
        "INTEGER": "Integer (whole number)",
        "REAL": "Decimal (floating-point number)",
        "TEXT": "Text (string)",
        "BOOLEAN": "Boolean (True/False flag)",
        "DATE": "Date (YYYY-MM-DD)",
        "": "Text (string)",
    }

    report = f"## 📊 Data Model: {table_name}\n"
    if catalog:
        ttype = catalog[2]
        icon = "📦" if ttype == "FACT" else "🧩"
        report += f"{icon} **Type:** {ttype} | **ETL Pipeline:** `{catalog[4]}`\n\n"
        report += f"### 📋 Business Description\n{catalog[5]}\n\n"
        report += f"**Source System:** `{catalog[3]}`\n"
        report += f"**Update Strategy:** {catalog[6]}\n"
        report += f"**Linked Tables:** {catalog[7]}\n\n"

        if catalog[8]:
            report += "### 📐 Key Measures & Derivations\n"
            report += "> These are the calculated or tracked values that power reporting and analytics for this table.\n\n"
            report += "| Measure | Formula / Source | What it means |\n"
            report += "|---|---|---|\n"
            entries = catalog[8].split("||")
            for entry in entries:
                parts = entry.strip().split("|")
                if len(parts) == 3:
                    name, formula, description = parts
                    report += f"| **{name.strip()}** | `{formula.strip()}` | {description.strip()} |\n"
                elif len(parts) == 1:
                    report += f"| **{parts[0].strip()}** | — | — |\n"
            report += "\n"

    if columns:
        report += f"### 🏛️ Column Schema ({len(columns)} columns)\n"
        report += "> Each column below shows its name, data type, and where the data originates from.\n\n"
        lineage_map = {row[3]: (row[1], row[2], row[4]) for row in lineage_rows}
        for col in columns:
            col_name = col[1]
            raw_type = (col[2] or "").upper()
            friendly_type = type_labels.get(raw_type, raw_type if raw_type else "Text")
            src_info = lineage_map.get(col_name)
            if src_info:
                report += f" - `{col_name}` — **{friendly_type}** ← `{src_info[0]}.{src_info[1]}` — *{src_info[2]}*\n"
            else:
                report += f" - `{col_name}` — **{friendly_type}**\n"

    return report


@tool
def get_full_impact_analysis(table_name: str, column_name: str, change_type: str = "drop") -> str:
    """Enterprise impact analysis for a proposed column or table change.
    Covers: downstream DW tables, at-risk ETL pipelines, affected reports/dashboards,
    and historical audit context. change_type: 'drop', 'rename', 'datatype'."""
    conn = _get_conn()
    cursor = conn.cursor()

    if column_name and column_name != "N/A":
        cursor.execute("""
            SELECT target_table, target_column, transformation_rule
            FROM data_lineage_map 
            WHERE (source_table = ? AND source_column = ?) OR (target_table = ? AND target_column = ?)
        """, (table_name, column_name, table_name, column_name))
        downstream_tables = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT tc.etl_pipeline
            FROM data_lineage_map dlm
            JOIN table_catalog tc ON tc.table_name = dlm.target_table
            WHERE (dlm.source_table = ? AND dlm.source_column = ?) OR (dlm.target_table = ? AND dlm.target_column = ?)
        """, (table_name, column_name, table_name, column_name))
        downstream_pipelines = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("""
            SELECT DISTINCT report_name, business_owner
            FROM report_dependency
            WHERE (dw_table = ? AND dw_column = ?)
               OR dw_table IN (
                   SELECT target_table FROM data_lineage_map
                   WHERE (source_table = ? AND source_column = ?) OR (target_table = ? AND target_column = ?)
               )
        """, (table_name, column_name, table_name, column_name, table_name, column_name))
        reports = cursor.fetchall()
    else:
        cursor.execute("""
            SELECT target_table, target_column, transformation_rule
            FROM data_lineage_map 
            WHERE source_table = ? OR target_table = ?
        """, (table_name, table_name))
        downstream_tables = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT tc.etl_pipeline
            FROM data_lineage_map dlm
            JOIN table_catalog tc ON tc.table_name = dlm.target_table
            WHERE dlm.source_table = ? OR dlm.target_table = ?
        """, (table_name, table_name))
        downstream_pipelines = [r[0] for r in cursor.fetchall() if r[0]]

        # Allow pipelines to show up correctly as the target object in table-level scans
        cursor.execute("SELECT DISTINCT table_name FROM table_catalog WHERE etl_pipeline = ?", (table_name,))
        pl_targets = [r[0] for r in cursor.fetchall() if r[0]]
        if pl_targets:
            downstream_pipelines.append(table_name)
            for pt in pl_targets:
                downstream_tables.append((pt, "ALL", "Pipeline Dependency"))

        cursor.execute("""
            SELECT DISTINCT report_name, business_owner
            FROM report_dependency
            WHERE dw_table = ?
               OR dw_table IN (
                   SELECT target_table FROM data_lineage_map
                   WHERE source_table = ? OR target_table = ?
               )
        """, (table_name, table_name, table_name))
        reports = cursor.fetchall()

    cursor.execute("""
        SELECT event_time, event_type, changed_by_user, change_description
        FROM db_audit_log WHERE target_object = ? OR target_object LIKE ?
        ORDER BY event_time DESC LIMIT 5
    """, (table_name, f"{table_name}.%"))
    audit_history = cursor.fetchall()

    conn.close()

    risk = "🔴 CRITICAL" if "drop" in change_type or "rename" in change_type else "🟡 MODERATE"
    report = f"## {risk} Impact Analysis: `{table_name}.{column_name}` [{change_type.upper()}]\n\n"

    if downstream_tables:
        report += f"### ⬇️ Downstream DW Tables ({len(downstream_tables)} affected)\n"
        for row in downstream_tables:
            report += f" - **{row[0]}**: `{row[1]}` — *{row[2]}*\n"
    else:
        report += "### ⬇️ Downstream DW Tables\n✅ No direct dependencies found.\n"

    if downstream_pipelines:
        report += f"\n### ⚙️ ETL Pipelines at Risk\n"
        for pl in downstream_pipelines:
            report += f" - `{pl}` — will fail if column is {change_type}d.\n"

    if reports:
        report += "### 📊 Downstream Report Impact\n"
        for rep in reports:
            report += f" - **{rep[0]}** (Owner: {rep[1]})\n"
    else:
        report += "\n### 📊 Reports & Dashboards\n✅ No registered report dependencies.\n"

    if audit_history:
        report += "\n### 🕐 Historical Audit Context\n"
        for row in audit_history:
            report += f" - [{row[0][:16]}] **{row[1]}** by `{row[2]}`: {row[3]}\n"

    prediction = (
        "All dependent ETL pipelines will fail immediately and reports will display errors."
        if "drop" in change_type or "rename" in change_type
        else "Type-casting errors or precision loss may occur in downstream calculations."
    )
    report += f"\n---\n**Agent Prediction:** {prediction}"
    return report


@tool
def generate_e2e_lineage_graph(table_name: str) -> str:
    """Generates a focused end-to-end lineage graph for a specific entity.
    Filters to only the rows where source_table OR target_table matches.
    Color-coded: Green=Source, Orange=ETL, Blue=DW, Red=selected node, Purple=BI Report."""
    conn = _get_conn()
    cursor = conn.cursor()

    # Normalise: try exact match first, then case-insensitive LIKE
    cursor.execute("""
        SELECT dlm.source_system, dlm.source_table, tc.etl_pipeline, dlm.target_table
        FROM data_lineage_map dlm
        LEFT JOIN table_catalog tc ON tc.table_name = dlm.target_table
        WHERE LOWER(dlm.source_table)  = LOWER(?)
           OR LOWER(dlm.target_table)  = LOWER(?)
    """, (table_name, table_name))
    rows = cursor.fetchall()

    # If no rows matched (e.g. the entity is a source system like "OLTP"), broaden to system match
    if not rows:
        cursor.execute("""
            SELECT dlm.source_system, dlm.source_table, tc.etl_pipeline, dlm.target_table
            FROM data_lineage_map dlm
            LEFT JOIN table_catalog tc ON tc.table_name = dlm.target_table
            WHERE LOWER(dlm.source_system) LIKE LOWER(?)
        """, (f"%{table_name}%",))
        rows = cursor.fetchall()

    # FEB FIX: If still no rows, check if it is a BI Report in report_dependency
    if not rows:
        cursor.execute("SELECT DISTINCT dw_table FROM report_dependency WHERE LOWER(report_name) = LOWER(?)", (table_name,))
        rep_tables = [r[0] for r in cursor.fetchall() if r[0]]
        if rep_tables:
             dw_tab = rep_tables[0]
             cursor.execute("""
                 SELECT dlm.source_system, dlm.source_table, tc.etl_pipeline, dlm.target_table
                 FROM data_lineage_map dlm
                 LEFT JOIN table_catalog tc ON tc.table_name = dlm.target_table
                 WHERE LOWER(dlm.source_table)  = LOWER(?)
                    OR LOWER(dlm.target_table)  = LOWER(?)
             """, (dw_tab, dw_tab))
             rows = cursor.fetchall()

    # Fetch downstream BI reports for the target tables in scope

    target_tables_in_scope = list({r[3] for r in rows if r[3]})
    report_map = {}  # target_table -> list of report names
    for tbl in target_tables_in_scope:
        cursor.execute("SELECT DISTINCT report_name FROM report_dependency WHERE dw_table = ?", (tbl,))
        reports = [r[0] for r in cursor.fetchall()]
        if reports:
            report_map[tbl] = reports

    conn.close()

    if not rows:
        return f"VISUALIZATION ERROR: No lineage data found for '{table_name}'."

    net = Network(height="600px", width="100%", bgcolor="#FFFFFF", font_color="#0F172A", directed=True)
    net.set_options(json.dumps({
        "physics": {"enabled": False},
        "layout": {
            "hierarchical": {
                "enabled": True, "direction": "LR", "sortMethod": "directed",
                "levelSeparation": 260, "nodeSpacing": 90, "treeSpacing": 120,
                "blockShifting": True, "edgeMinimization": True
            }
        },
        "interaction": {"navigationButtons": True, "keyboard": True, "zoomView": True}
    }))

    added_nodes = set()

    def add_node(node_id, label, color, shape, title, size=18):
        if node_id not in added_nodes:
            net.add_node(node_id, label=label, color={"background": color, "border": "#fff",
                         "highlight": {"background": "#FBBF24"}},
                         shape=shape, title=title, size=size,
                         font={"size": 13, "color": "#0F172A", "bold": True})
            added_nodes.add(node_id)

    for row in rows:
        src_system, src_table, etl_pl, tgt_table = row
        src_system = src_system or "Unknown Source"
        etl_pl     = etl_pl or "ETL_PIPELINE"

        add_node(src_system, src_system.split(":")[0], "#059669", "database",
                 f"Source System: {src_system}", size=24)

        src_key = f"{src_system}::{src_table}"
        add_node(src_key, src_table, "#34D399", "ellipse",
                 f"Source Table: {src_table}\nSystem: {src_system}")

        add_node(etl_pl, etl_pl.replace("_", "\n"), "#F59E0B", "box",
                 f"ETL Pipeline: {etl_pl}", size=20)

        is_selected = tgt_table and (tgt_table.lower() == table_name.lower()
                                     or src_table.lower() == table_name.lower())
        tgt_color = "#EF4444" if is_selected else "#3B82F6"
        if tgt_table:
            add_node(tgt_table, tgt_table, tgt_color, "ellipse",
                     f"DW Table: {tgt_table}\n{'▶ Queried entity' if is_selected else ''}", size=20)

        net.add_edge(src_system, src_key, color="#6EE7B7", title="contains", width=1.5)
        net.add_edge(src_key, etl_pl,     color="#FDE68A", title="feeds into", width=1.5)
        if tgt_table:
            net.add_edge(etl_pl, tgt_table, color="#93C5FD", title="loads to", width=2)

    # Add BI report nodes as final downstream layer
    for tbl, reports in report_map.items():
        for rep in reports:
            rep_id = f"REP::{rep}"
            short  = rep[:22] + "..." if len(rep) > 22 else rep
            
            # FEB FIX: Highlight report node if it was the queried entity
            is_rep_selected = (rep.lower() == table_name.lower())
            rep_color = "#EF4444" if is_rep_selected else "#8B5CF6"
            
            add_node(rep_id, short, rep_color, "diamond",
                     f"BI Report: {rep}", size=16)
            net.add_edge(tbl, rep_id, color="#C4B5FD", title="powers", width=1.5, dashes=True)

    output_path = os.path.join(OUTPUT_DIR, f"{table_name}_e2e_lineage.html")

    net.save_graph(output_path)
    return f"End-to-End lineage graph generated. View it at: {output_path}"



@tool
def get_business_context(query: str) -> str:
    """Retrieves highly relevant historical business context, past ETL pipeline failures, or
    documentation snippets using Semantic RAG (Retrieval-Augmented Generation). Use this tool 
    when asked about 'past issues', 'historical context', or 'why did this pipeline fail'."""
    if not chromadb:
        return "RAG Context Engine is disabled (ChromaDB not installed). Please install 'chromadb'."
        
    try:
        # Connect to ChromaDB
        os.makedirs(CHROMA_DB_DIR, exist_ok=True)
        client = chromadb.PersistentClient(path=CHROMA_DB_DIR)
        collection = client.get_or_create_collection(name="enterprise_knowledge")
        
        # We assume knowledge was seeded dynamically, but we'll seed on the fly if empty for the demo
        if collection.count() == 0:
            seed_rag_knowledge(collection)
            
        results = collection.query(
            query_texts=[query],
            n_results=3
        )
        
        if not results['documents'] or not results['documents'][0]:
            return "No historical business context found for that query."
            
        context = "### 🧠 Retrieved Business Context (RAG)\n"
        for i, doc in enumerate(results['documents'][0]):
            meta = results['metadatas'][0][i]
            context += f"- **[{meta.get('source', 'Knowledge Base')}]** {doc}\n"
            
        return context
        
    except Exception as e:
        return f"Error retrieving context via RAG: {e}"

def seed_rag_knowledge(collection):
    """Dynamically build the vector database from RDBMS metadata and logs."""
    conn = _get_conn()
    cursor = conn.cursor()
    
    docs = []
    metas = []
    ids = []
    
    # Grab failed ETL logs
    cursor.execute("SELECT pipeline_name, start_time, error_message FROM etl_execution_logs WHERE status = 'FAILED'")
    for i, row in enumerate(cursor.fetchall()):
        docs.append(f"Pipeline '{row[0]}' failed on {row[1][:10]} due to: {row[2]}. This occasionally happens with invalid source files or network timeouts.")
        metas.append({"source": "ETL Logs", "type": "Failure History"})
        ids.append(f"etl_fail_{i}")
        
    # Grab Pipeline Audit summaries
    cursor.execute("SELECT pipeline_name, version_tag, change_summary FROM etl_pipeline_audit")
    for i, row in enumerate(cursor.fetchall()):
        docs.append(f"The ETL pipeline '{row[0]}' was updated to {row[1]}. Release notes: {row[2]}.")
        metas.append({"source": "Audit DB", "type": "ETL Versioning"})
        ids.append(f"etl_audit_{i}")
        
    if docs:
        collection.add(documents=docs, metadatas=metas, ids=ids)
    conn.close()

@tool
def get_holistic_entity_context(entity_name: str) -> str:
    """MASTER TOOL: Universally analyze any entity (Source, Table, Pipeline, or Report).
    Returns complete bi-directional lineage, impact intelligence, audit history, operational logs,
    and actionable recommendations in a single comprehensive response."""
    conn = _get_conn()
    cursor = conn.cursor()
    
    # 1. Determine entity type (using case-insensitive LOWER matches)
    entity_type = "UNKNOWN"
    entity_lower = entity_name.lower()
    
    cursor.execute("SELECT table_name FROM table_catalog WHERE LOWER(table_name) = ?", (entity_lower,))
    cat = cursor.fetchone()
    if cat: 
        entity_name = cat[0]
        entity_type = "TABLE"
    else:
        cursor.execute("SELECT DISTINCT source_system FROM data_lineage_map WHERE LOWER(source_system) = ?", (entity_lower,))
        src = cursor.fetchone()
        if src:
            entity_name = src[0]
            entity_type = "SOURCE"
    
    if entity_type == "UNKNOWN":
        cursor.execute("SELECT DISTINCT etl_pipeline FROM table_catalog WHERE LOWER(etl_pipeline) = ?", (entity_lower,))
        pl = cursor.fetchone()
        if pl:
            entity_name = pl[0]
            entity_type = "PIPELINE"
            
    if entity_type == "UNKNOWN":
        cursor.execute("SELECT DISTINCT report_name FROM report_dependency WHERE LOWER(report_name) = ?", (entity_lower,))
        rep = cursor.fetchone()
        if rep:
            entity_name = rep[0]
            entity_type = "REPORT"
            
    if entity_type == "UNKNOWN":
         cursor.execute("SELECT DISTINCT target_table FROM data_lineage_map WHERE LOWER(target_table) LIKE ? OR LOWER(source_table) LIKE ?", (f"%{entity_lower}%", f"%{entity_lower}%"))
         match = cursor.fetchone()
         if match: 
             entity_name = match[0]
             entity_type = "TABLE"
         else:
             return f"ENTITY NOT FOUND: Cannot locate '{entity_name}' in the ecosystem metadata structures."


    report = f"# 🌐 Holistic Ecosystem Context: `{entity_name}` ({entity_type})\n\n"
    
    # --- 2. BI-DIRECTIONAL LINEAGE & COMPLETE CHAIN ---
    if entity_type in ["TABLE", "SOURCE"]:
        report += "### 🗺️ Full End-to-End Data Lineage\n"
        
        # A. UPSTREAM LINEAGE (Where does this table get its data?)
        cursor.execute("""
            SELECT dlm.source_system, dlm.source_table, tc.etl_pipeline, dlm.transformation_rule 
            FROM data_lineage_map dlm
            LEFT JOIN table_catalog tc ON tc.table_name = dlm.target_table
            WHERE dlm.target_table = ?
        """, (entity_name,))
        upstream = cursor.fetchall()
        
        if upstream:
            report += "#### 🔼 Upstream Lineage (Source ➔ Staging ➔ DW)\n"
            for row in upstream:
                report += f"- **Source:** `[{row[0]}] {row[1]}`\n"
                report += f"  - **ETL Pipeline:** `{row[2] or 'N/A'}`\n"
                if row[3]: report += f"  - **Rule:** *{row[3]}*\n"
        
        # B. DOWNSTREAM LINEAGE (Who consumes data from this table?)
        cursor.execute("""
            SELECT target_table, transformation_rule 
            FROM data_lineage_map 
            WHERE source_table = ?
        """, (entity_name,))
        downstream = cursor.fetchall()
        
        if downstream:
            report += "\n#### 🔽 Downstream Lineage (DW ➔ Data Marts / Aggregates)\n"
            for row in downstream:
                report += f"- **Feeds into:** `{row[0]}`\n"
                if row[1]: report += f"  - **Rule:** *{row[1]}*\n"

        # C. BUSINESS DEPENDENCIES (BI Reports)
        cursor.execute("SELECT DISTINCT report_name, business_owner FROM report_dependency WHERE dw_table = ?", (entity_name,))
        reports = cursor.fetchall()
        if reports:
            report += "\n#### 📊 Downstream BI Reports & consumption\n"
            for r in reports:
                report += f"- `📊 {r[0]}` (Business Owner: **{r[1]}**)\n"

    elif entity_type == "REPORT":
        cursor.execute("SELECT dw_table, dw_column, transformation_rule FROM report_dependency WHERE report_name = ?", (entity_name,))
        deps = cursor.fetchall()
        report += "### 🗺️ Bottom-Up Lineage (Report ➔ Source)\n"
        for d in deps:
            dw_table = d[0]
            report += f"**Uses Target Table:** `{dw_table}` (Column: `{d[1]}`)\n"
            
            cursor.execute("""
                SELECT source_system, source_table, tc.etl_pipeline 
                FROM data_lineage_map dlm 
                JOIN table_catalog tc ON tc.table_name = dlm.target_table 
                WHERE dlm.target_table = ?
            """, (dw_table,))
            srcs = cursor.fetchall()
            for s in srcs:
                report += f"  ↳ **Fed by Source:** `[{s[0]}] {s[1]}` via Pipeline: `{s[2]}`\n"
                
    elif entity_type == "PIPELINE":
        cursor.execute("SELECT source_system, source_table, target_table FROM data_lineage_map dlm JOIN table_catalog tc ON tc.table_name = dlm.target_table WHERE tc.etl_pipeline = ?", (entity_name,))
        flow = cursor.fetchall()
        report += "### 🗺️ Pipeline Orchestration Flow\n"
        for f in flow:
            report += f"- Extracts from `{f[0]}` (`{f[1]}`) ➔ Loads to `{f[2]}`\n"
            
    # --- 3. OPERATIONAL HEALTH & ETL LOGS ---
    search_pl = entity_name if entity_type == "PIPELINE" else None
    if not search_pl and entity_type == "TABLE":
        cursor.execute("SELECT etl_pipeline FROM table_catalog WHERE table_name = ?", (entity_name,))
        pl = cursor.fetchone()
        if pl: search_pl = pl[0]
        
    if search_pl:
        cursor.execute("SELECT start_time, status, records_inserted, error_message, retry_attempts FROM etl_execution_logs WHERE pipeline_name = ? ORDER BY start_time DESC LIMIT 5", (search_pl,))
        logs = cursor.fetchall()
        report += "\n### ⚙️ Operational Health & ETL Status\n"
        report += f"**Active Pipeline:** `{search_pl}`\n\n"
        if logs:
            has_failures = False
            for l in logs:
                icon = "✅ SUCCESS" if l[1] == "SUCCESS" else "🔴 FAILED"
                if l[1] == "FAILED": has_failures = True
                report += f"- **{icon}** | `{l[0][:16]}` | Records Ingested: **{l[2]}** | Retries: {l[4]}\n"
                if l[1] == "FAILED": report += f"  - *Error Reason:* `{l[3]}`\n"
                
            if has_failures:
                report += "\n**🤖 Automatic AI Recommendation:**\n"
                report += "> I have detected recent pipeline failure alerts. This is often caused by source schema drifts. **Do you want me to dynamically analyze the schema drift adjustments?** *(Reply: 'Go ahead and do this' to execute).* \n"
        else:
            report += "No execution logs found for this pipeline.\n"
            
    # --- 4. IMPACT ASSESSMENT ---
    if entity_type == "TABLE":
        report += "\n### ⚠️ Real-Time Impact Assessment\n"
        cursor.execute("SELECT COUNT(DISTINCT report_name) FROM report_dependency WHERE dw_table = ?", (entity_name,))
        rep_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM data_lineage_map WHERE source_table = ?", (entity_name,))
        down_deps = cursor.fetchone()[0]
        
        if rep_count > 0 or down_deps > 0:
            report += f"**CRITICAL IMPACT RISK SCORE: HIGH**\n"
            if down_deps > 0:
                report += f"- Breaks **{down_deps}** downstream staging/fact tables.\n"
            if rep_count > 0:
                report += f"- Disrupts **{rep_count}** downstream BI reporting layouts.\n"
        else:
            report += "Low Impact Risk: This table has no recorded downstream dependents. Safe to modify.\n"
            
    # --- 5. AUDIT & CHANGE LOG DETAILS ---
    cursor.execute("SELECT event_time, event_type, changed_by_user, change_description FROM db_audit_log WHERE target_object LIKE ? ORDER BY event_time DESC LIMIT 5", (f"%{entity_name}%",))
    audits = cursor.fetchall()
    if audits:
        report += "\n### 🛡️ Unified Audit & Change Logs\n"
        for a in audits:
             report += f"- **[{a[0][:16]}] {a[1]}** by `{a[2]}`: {a[3]}\n"
             
    try:
        generate_e2e_lineage_graph.run(entity_name)
    except Exception:
        pass
        
    report += f"\n\n[Agent Automated Action]: View graphical lineage at: {os.path.join(OUTPUT_DIR, f'{entity_name}_e2e_lineage.html')}"
    conn.close()
    return report



def generate_ecosystem_overview_graph() -> str:
    """Generates a full 4-tier ecosystem overview lineage graph:
    Source Systems → ETL Pipelines → DW Tables → BI Reports.
    Color-coded by tier. Saved as HTML and returns the file path."""
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT source_system, source_table, target_table FROM data_lineage_map")
    lineage_rows = cursor.fetchall()

    cursor.execute("SELECT table_name, etl_pipeline FROM table_catalog WHERE etl_pipeline IS NOT NULL")
    pipeline_map = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT DISTINCT report_name, dw_table, business_owner FROM report_dependency")
    report_rows = cursor.fetchall()

    conn.close()

    net = Network(height="620px", width="100%", bgcolor="#0F172A", font_color="#F8FAFC", directed=True)
    net.set_options(json.dumps({
        "physics": {"enabled": True, "stabilization": {"iterations": 100}},
        "layout": {"hierarchical": {"enabled": True, "direction": "LR", "sortMethod": "directed", "levelSeparation": 220, "nodeSpacing": 80}},
        "interaction": {"navigationButtons": True, "zoomView": True, "keyboard": True},
        "edges": {"arrows": {"to": {"enabled": True, "scaleFactor": 0.8}}, "smooth": {"type": "cubicBezier"}, "color": {"inherit": False}}
    }))

    added = set()

    def add_node(node_id, label, color, shape, title, level, size=22):
        if node_id not in added:
            net.add_node(node_id, label=label, color={"background": color, "border": "#FFFFFF", "highlight": {"background": "#FBBF24"}},
                         shape=shape, title=title, level=level, size=size, font={"size": 13, "color": "#F8FAFC", "bold": True})
            added.add(node_id)

    # Layer 1: Source Systems
    src_systems = set()
    for src_sys, src_tbl, tgt_tbl in lineage_rows:
        if src_sys:
            src_systems.add(src_sys)
    for ss in src_systems:
        clean = ss.split(":")[0] if ":" in ss else ss
        add_node(f"SYS_{ss}", clean, "#059669", "database", f"Source System: {ss}", level=0, size=28)

    # Layer 2: Source Tables
    src_tables = {}
    for src_sys, src_tbl, tgt_tbl in lineage_rows:
        if src_tbl and src_sys:
            key = f"SRCT_{src_sys}_{src_tbl}"
            if key not in src_tables:
                src_tables[key] = (src_sys, src_tbl)
    for key, (src_sys, src_tbl) in src_tables.items():
        add_node(key, src_tbl, "#34D399", "ellipse", f"Source Table: {src_tbl}\nSystem: {src_sys}", level=1)
        net.add_edge(f"SYS_{src_sys}", key, color="#6EE7B7", title="contains", width=1.5)

    # Layer 3: ETL Pipelines
    etl_nodes = {}
    for src_sys, src_tbl, tgt_tbl in lineage_rows:
        pl = pipeline_map.get(tgt_tbl, "ETL_PIPELINE")
        etl_key = f"ETL_{pl}"
        if etl_key not in etl_nodes:
            etl_nodes[etl_key] = pl
            add_node(etl_key, pl.replace("_", "\n"), "#F59E0B", "box", f"ETL Pipeline: {pl}", level=2, size=26)
        src_key = f"SRCT_{src_sys}_{src_tbl}"
        edge_id = f"{src_key}__{etl_key}"
        if edge_id not in added:
            net.add_edge(src_key, etl_key, color="#FDE68A", title="feeds", width=1.5)
            added.add(edge_id)

    # Layer 4: DW Tables
    dw_tables = set(r[2] for r in lineage_rows if r[2])
    for tgt in dw_tables:
        add_node(f"DW_{tgt}", tgt, "#3B82F6", "ellipse", f"DW Table: {tgt}", level=3, size=24)
        pl = pipeline_map.get(tgt, "ETL_PIPELINE")
        etl_key = f"ETL_{pl}"
        edge_id = f"{etl_key}__{tgt}"
        if edge_id not in added:
            net.add_edge(etl_key, f"DW_{tgt}", color="#93C5FD", title="loads to", width=2)
            added.add(edge_id)

    # Layer 5: BI Reports
    seen_reports = set()
    for rep_name, dw_tbl, owner in report_rows:
        if rep_name not in seen_reports:
            short = rep_name[:20] + "..." if len(rep_name) > 20 else rep_name
            add_node(f"REP_{rep_name}", short, "#8B5CF6", "diamond",
                     f"Report: {rep_name}\nOwner: {owner}", level=4, size=22)
            seen_reports.add(rep_name)
        edge_id = f"DW_{dw_tbl}__REP_{rep_name}"
        if edge_id not in added and f"DW_{dw_tbl}" in added:
            net.add_edge(f"DW_{dw_tbl}", f"REP_{rep_name}", color="#C4B5FD", title="powers", width=1.5, dashes=True)
            added.add(edge_id)

    output_path = os.path.join(OUTPUT_DIR, "ecosystem_overview.html")
    net.save_graph(output_path)
    return output_path

