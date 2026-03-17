import sqlite3
import pandas as pd
import os

DB_PATH = 'data/target_dw/target_system.db'
OUTPUT_DIR = 'github_assets'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def export_table_to_csv(conn, table_name):
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        out_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
        df.to_csv(out_path, index=False)
        print(f"Exported {table_name} to {out_path}")
    except Exception as e:
        print(f"Error exporting {table_name}: {e}")

def main():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # Tables to export for the demo
    tables = [
        "data_lineage_map", 
        "etl_execution_logs", 
        "db_audit_log", 
        "report_dependency",
        "table_catalog",
        "Dim_Customer",
        "Dim_Product",
        "Fact_Sales"
    ]
    
    for tbl in tables:
        export_table_to_csv(conn, tbl)
        
    conn.close()

if __name__ == "__main__":
    main()
