import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "org_automotive_env.db")

def create_automotive_db():
    print(f"Creating org_automotive_env.db at {DB_PATH}...")
    if os.path.exists(DB_PATH): os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS data_lineage_map (id INTEGER PRIMARY KEY, source_system TEXT, source_table TEXT, source_column TEXT, target_table TEXT, target_column TEXT, transformation_logic TEXT);
        CREATE TABLE IF NOT EXISTS table_catalog (table_name TEXT, column_name TEXT, data_type TEXT, is_pii BOOLEAN, etl_pipeline TEXT);
        CREATE TABLE IF NOT EXISTS etl_execution_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_name TEXT, mapping_name TEXT, pipeline_name TEXT, source_system TEXT, target_table TEXT, start_time TEXT, end_time TEXT, records_read INTEGER, records_inserted INTEGER, records_updated INTEGER, transformation_metrics TEXT, status TEXT, error_message TEXT, notes TEXT, db_audit_ref TEXT);
        CREATE TABLE IF NOT EXISTS db_audit_log (audit_id TEXT, event_time TEXT, event_type TEXT, target_object TEXT, changed_by_user TEXT, user_role TEXT, access_type TEXT, environment TEXT, change_description TEXT);
        CREATE TABLE IF NOT EXISTS asset_access_control (asset_name TEXT, asset_type TEXT, user_group TEXT, user_email TEXT, environment TEXT, account_type TEXT, access_level TEXT, granted_date TEXT);
        CREATE TABLE IF NOT EXISTS report_dependency (report_name TEXT, business_owner TEXT, target_audience TEXT, dw_table TEXT, metrics_kpis TEXT, usage_frequency TEXT, run_count INTEGER, last_refreshed TEXT);
        CREATE TABLE IF NOT EXISTS bi_report_usage (report_name TEXT, user_group TEXT, user_email TEXT, access_level TEXT, run_count INTEGER, last_run_timestamp TEXT, refresh_frequency TEXT);

        INSERT INTO data_lineage_map (source_system, source_table, source_column, target_table, target_column, transformation_logic) VALUES
        ('IoT Hub', 'vehicle_telemetry', 'vin_number', 'fact_sensor_data', 'vehicle_id', 'Direct Map'),
        ('IoT Hub', 'vehicle_telemetry', 'engine_temp', 'fact_sensor_data', 'temperature_c', 'F_to_C_Conversion'),
        ('Factory DB', 'assembly_line_log', 'batch_code', 'dim_manufacturing', 'production_batch', 'Direct Map'),
        ('Sales API', 'dealership_sales', 'dealer_id', 'fact_auto_sales', 'dealer_sk', 'Hash'),
        ('CSV Export', 'warranty_claims.csv', 'claim_id', 'dim_warranty', 'claim_id', 'Direct Map');

        INSERT INTO table_catalog (table_name, column_name, data_type, is_pii, etl_pipeline) VALUES
        ('fact_sensor_data', 'vehicle_id', 'VARCHAR', 1, 'Telemetry_Stream_ETL'),
        ('dim_manufacturing', 'production_batch', 'VARCHAR', 0, 'Factory_Sync_Batch'),
        ('fact_auto_sales', 'dealer_sk', 'VARCHAR', 0, 'Dealer_API_Pulse'),
        ('dim_warranty', 'claim_id', 'INT', 1, 'Warranty_Ingestion'),
        ('vehicle_telemetry', 'vin_number', 'VARCHAR', 1, 'N/A'),
        ('assembly_line_log', 'batch_code', 'VARCHAR', 0, 'N/A'),
        ('dealership_sales', 'dealer_id', 'VARCHAR', 0, 'N/A'),
        ('warranty_claims.csv', 'claim_id', 'INT', 1, 'N/A');

        INSERT INTO etl_execution_logs (workflow_name, mapping_name, pipeline_name, source_system, target_table, start_time, end_time, records_read, records_inserted, records_updated, status, notes, db_audit_ref) VALUES
        ('WF_Telemtry', 'MAP_SENSORS', 'Telemetry_Stream_ETL', 'IoT Hub', 'fact_sensor_data', '2026-03-23 01:00:00', '2026-03-23 01:05:00', 4500000, 4500000, 0, 'SUCCESS', 'Ingested worldwide telematics', 'AUD-A1'),
        ('WF_Factory', 'MAP_ASSEMBLY', 'Factory_Sync_Batch', 'Factory DB', 'dim_manufacturing', '2026-03-23 02:00:00', '2026-03-23 02:30:00', 85000, 85000, 0, 'SUCCESS', 'Daily manufacturing log imported', 'AUD-A2'),
        ('WF_Dealer', 'MAP_SALES', 'Dealer_API_Pulse', 'Sales API', 'fact_auto_sales', '2026-03-23 03:00:00', '2026-03-23 03:10:00', 12000, 11500, 500, 'SUCCESS', 'Dealership network sync', 'AUD-A3'),
        ('WF_Warranty', 'MAP_CLAIMS', 'Warranty_Ingestion', 'CSV Export', 'dim_warranty', '2026-03-23 04:00:00', '2026-03-23 04:05:00', 350, 0, 0, 'FAILED', 'Corrupted CSV headers', 'AUD-A4');

        INSERT INTO db_audit_log (audit_id, event_time, event_type, target_object, changed_by_user, user_role, access_type, environment, change_description) VALUES
        ('AUD-A1', '2026-03-23 01:00:00', 'INSERT', 'fact_sensor_data', 'svc_telemetry', 'System', 'Write', 'PROD', 'Bulk sensor load'),
        ('AUD-A2', '2026-03-23 02:00:00', 'INSERT', 'dim_manufacturing', 'svc_factory', 'System', 'Write', 'PROD', 'Assembly line sync'),
        ('AUD-A3', '2026-03-23 03:00:00', 'UPDATE', 'fact_auto_sales', 'svc_dealer', 'System', 'Write', 'PROD', 'Sales adjustments applied'),
        ('AUD-A4', '2026-03-23 04:00:00', 'ALERT', 'dim_warranty', 'svc_warranty', 'System', 'Write', 'PROD', 'Data load aborted strictly due to schema mismatch');

        INSERT INTO asset_access_control (asset_name, asset_type, user_group, user_email, environment, account_type, access_level, granted_date) VALUES
        ('fact_sensor_data', 'Target DW Table', 'R&D Engineering', 'rnd@auto.com', 'PROD', 'Group', 'Read', '2024-01-01'),
        ('dim_manufacturing', 'Target DW Table', 'Production Ops', 'ops@auto.com', 'PROD', 'Group', 'Read/Write', '2024-02-15'),
        ('fact_auto_sales', 'Target DW Table', 'Global Sales', 'sales@auto.com', 'PROD', 'Group', 'Read', '2024-03-10'),
        ('vehicle_telemetry', 'RDBMS Source', 'IoT Core Sys', 'core@auto.com', 'PROD', 'Group', 'Admin', '2023-11-01');

        INSERT INTO report_dependency (report_name, business_owner, target_audience, dw_table, metrics_kpis, usage_frequency, run_count, last_refreshed) VALUES
        ('Fleet Health Monitor', 'VP Engineering', 'R&D Team', 'fact_sensor_data', 'Engine Overheating Incidents', 'Hourly', 8400, '2026-03-23'),
        ('Global Auto Sales', 'Sales Director', 'Executives', 'fact_auto_sales', 'Vehicles Sold, Regional Quota', 'Daily', 1500, '2026-03-23'),
        ('Factory Throughput', 'Plant Manager', 'Assembly Floor', 'dim_manufacturing', 'Units per Hour', 'Hourly', 4500, '2026-03-23');

        INSERT INTO bi_report_usage (report_name, user_group, user_email, access_level, run_count, last_run_timestamp, refresh_frequency) VALUES
        ('Fleet Health Monitor', 'Engineers', 'eng_lead@auto.com', 'Editor', 4200, '2026-03-23 08:00:00', 'Hourly'),
        ('Global Auto Sales', 'C-Suite', 'vp_sales@auto.com', 'Viewer', 800, '2026-03-23 09:00:00', 'Daily'),
        ('Factory Throughput', 'Plant Supervisors', 'plant_1@auto.com', 'Viewer', 3000, '2026-03-23 10:00:00', 'Hourly');
    """)

    conn.commit()
    conn.close()
    print("Database org_automotive_env.db heavily populated and created.")

if __name__ == '__main__':
    create_automotive_db()
