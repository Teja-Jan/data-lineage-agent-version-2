import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')

def create_reporting_views():
    print(f"Creating SQL Views for Reporting in {TARGET_DB_PATH}...")
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()

    # View 1: ETL Pipeline Health (For BI Dashboards)
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS vw_etl_pipeline_health AS
        SELECT 
            pipeline_name,
            COUNT(log_id) as total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
            SUM(records_inserted) as total_records_inserted,
            MAX(end_time) as last_run_time
        FROM etl_execution_logs
        GROUP BY pipeline_name
    ''')

    # View 2: Database Audit & Permissions Dashboard
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS vw_db_audit_summary AS
        SELECT 
            target_object as table_name,
            event_type,
            COUNT(audit_id) as event_count,
            MAX(event_time) as latest_event_time
        FROM db_audit_log
        GROUP BY target_object, event_type
    ''')

    # View 3: Data Lineage Matrix (Flat format for easy import into Power BI/Tableau)
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS vw_data_lineage_matrix AS
        SELECT 
            source_system,
            source_table,
            source_column,
            target_table,
            target_column,
            transformation_rule
        FROM data_lineage_map
    ''')

    conn.commit()
    conn.close()
    print("SQL Views Successfully Created.")

if __name__ == "__main__":
    create_reporting_views()
