import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = 'data/target_dw/target_system.db'

def update_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Add retry_attempts to etl_execution_logs if not exists
    try:
        cursor.execute("ALTER TABLE etl_execution_logs ADD COLUMN retry_attempts INTEGER DEFAULT 0")
        print("Added retry_attempts column.")
    except sqlite3.OperationalError:
        print("retry_attempts column already exists.")

    # 2. Insert some failed ETL data for demonstration
    pipelines = ["SAP_TO_DW_PRODUCT", "SFDC_TO_DW_ACCOUNT", "WEB_TO_DW_SESSIONS"]
    now = datetime.now()
    
    for i, pl in enumerate(pipelines):
        start = (now - timedelta(hours=i+1)).strftime("%Y-%m-%d %H:%M:%S")
        end = (now - timedelta(hours=i, minutes=45)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO etl_execution_logs (pipeline_name, source_system, target_table, start_time, end_time, records_read, records_inserted, status, error_message, retry_attempts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pl, "External_API", "N/A", start, end, 1000, 0, "FAILED", f"Connection timeout after 30s. Peer reset connection. [Code: ERR_{i+500}]", i+1))
    
    conn.commit()
    conn.close()
    print("Database updated with failure demo data.")

if __name__ == "__main__":
    update_db()
