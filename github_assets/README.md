# 📂 GitHub Demo Assets

This folder contains **LIVE CSV exports** of the internal governance Data Warehouse tables. 

Because standard GitHub interfaces cannot render `.db` binary SQLite files natively, we have exported key audit tables as CSV spreadsheets for **direct click-through viewing** in your demo.

### 📊 Tables Avaiable:

| File | Description | Business Purpose |
| :--- | :--- | :--- |
| **`data_lineage_map.csv`** | Column-Level Lineage | Tracks exactly where target DB columns extract data from. |
| **`etl_execution_logs.csv`** | Live Pipeline Logs | Shows Success/Failure run history with error messages. |
| **`db_audit_log.csv`** | Schema Accountability | Records DDL statements and full account auditor scorecards. |
| **`table_catalog.csv`** | Static Glossary | Business documentation outlining description and source system. |
| **`report_dependency.csv`** | Transitive Mapping | Maps Downstream dashboard impacts to individual column structures. |

---

### 💡 Demo Tip:
You can use the **Search** bar at the top of any GitHub file page to quickly locate a specific keyword like "Fact" or "Dim" directly from the browser viewport interface.
