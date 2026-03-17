import sqlite3
import os

DB_PATH = 'data/target_dw/target_system.db'

def check_schema():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table_name, sql in tables:
        print(f"--- Table: {table_name} ---")
        print(sql)
        print()
        
    conn.close()

if __name__ == "__main__":
    check_schema()
