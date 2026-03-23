import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "org_test_env.db")

def create_mock_db():
    print(f"Creating org_test_env.db at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS data_lineage_map (
            id INTEGER PRIMARY KEY, source_system TEXT, source_table TEXT, source_column TEXT,
            target_table TEXT, target_column TEXT, transformation_logic TEXT
        );
        CREATE TABLE IF NOT EXISTS table_catalog (
            table_name TEXT, column_name TEXT, data_type TEXT, is_pii BOOLEAN, etl_pipeline TEXT
        );
        CREATE TABLE IF NOT EXISTS etl_execution_logs (
            run_id TEXT, pipeline_name TEXT, start_time TEXT, status TEXT, records_processed INTEGER, audit_ref TEXT
        );
        CREATE TABLE IF NOT EXISTS db_audit_log (
            audit_id TEXT, event_time TEXT, event_type TEXT, target_object TEXT, changed_by_user TEXT, user_role TEXT, access_type TEXT, environment TEXT, change_description TEXT
        );
        CREATE TABLE IF NOT EXISTS asset_access_control (
            asset_name TEXT, asset_type TEXT, user_group TEXT, user_email TEXT, environment TEXT, account_type TEXT, access_level TEXT, granted_date TEXT
        );
        CREATE TABLE IF NOT EXISTS report_dependency (
            report_name TEXT, business_owner TEXT, target_audience TEXT, dw_table TEXT, metrics_kpis TEXT, usage_frequency TEXT, run_count INTEGER, last_refreshed TEXT
        );
        CREATE TABLE IF NOT EXISTS bi_report_usage (
            report_name TEXT, user_group TEXT, user_email TEXT, access_level TEXT, run_count INTEGER, last_run_timestamp TEXT, refresh_frequency TEXT
        );
    """)

    # Populate Lineage Map (Retail System)
    cur.executescript("""
        INSERT INTO data_lineage_map (source_system, source_table, source_column, target_table, target_column, transformation_logic) VALUES
        ('PostgreSQL', 'retail_orders', 'order_id', 'fact_sales', 'sale_id', 'Direct Map'),
        ('PostgreSQL', 'retail_orders', 'total_amount', 'fact_sales', 'revenue', 'SUM()'),
        ('PostgreSQL', 'retail_orders', 'customer_id', 'fact_sales', 'customer_hash', 'SHA256'),
        ('MySQL DB', 'customer_profiles', 'cust_id', 'dim_customers', 'sys_cust_id', 'Direct Map'),
        ('MySQL DB', 'customer_profiles', 'email', 'dim_customers', 'contact', 'Masked'),
        ('Oracle Inventory', 'warehouse_stock', 'sku', 'fact_inventory', 'item_id', 'VLOOKUP Map'),
        ('Oracle Inventory', 'warehouse_stock', 'qty', 'fact_inventory', 'stock_count', 'SUM()'),
        ('REST API', 'stripe_payments', 'status', 'fact_sales', 'payment_status', 'CASE WHEN status=1 THEN Paid'),
        ('REST API', 'fedex_shipping_api', 'tracking', 'dim_shipments', 'tracking_no', 'Direct Map'),
        ('CSV File', 'returns_log.csv', 'return_id', 'dim_returns', 'return_id', 'Direct Map'),
        ('CSV File', 'historical_sales_2024.csv', 'total', 'fact_sales', 'revenue', 'Append Rule');
    """)

    # Populate Catalog
    cur.executescript("""
        INSERT INTO table_catalog (table_name, column_name, data_type, is_pii, etl_pipeline) VALUES
        ('fact_sales', 'sale_id', 'INT', 0, 'Retail_Nightly_Sync'),
        ('fact_sales', 'customer_hash', 'VARCHAR', 1, 'Retail_Nightly_Sync'),
        ('dim_returns', 'return_id', 'INT', 0, 'Retail_Returns_Sync'),
        ('dim_customers', 'contact', 'VARCHAR', 1, 'Customer_Master_ETL'),
        ('fact_inventory', 'stock_count', 'INT', 0, 'Inventory_Hourly_Delta'),
        ('dim_shipments', 'tracking_no', 'VARCHAR', 0, 'Logistics_API_Pulse'),
        ('retail_orders', 'order_id', 'INT', 0, 'N/A'),
        ('customer_profiles', 'cust_id', 'INT', 1, 'N/A'),
        ('warehouse_stock', 'sku', 'VARCHAR', 0, 'N/A'),
        ('stripe_payments', 'status', 'VARCHAR', 0, 'N/A'),
        ('fedex_shipping_api', 'tracking', 'VARCHAR', 0, 'N/A'),
        ('returns_log.csv', 'return_id', 'INT', 0, 'N/A'),
        ('historical_sales_2024.csv', 'total', 'FLOAT', 0, 'N/A');
    """)

    # Populate ETL Logs
    cur.executescript("""
        INSERT INTO etl_execution_logs (run_id, pipeline_name, start_time, status, records_processed, audit_ref) VALUES
        ('RUN-001', 'Retail_Nightly_Sync', '2026-03-22 08:00:00', 'SUCCESS', 150000, 'AUD-991'),
        ('RUN-002', 'Retail_Returns_Sync', '2026-03-22 08:30:00', 'SUCCESS', 1200, 'AUD-992'),
        ('RUN-003', 'Customer_Master_ETL', '2026-03-22 09:00:00', 'FAILED', 85000, 'AUD-993'),
        ('RUN-004', 'Inventory_Hourly_Delta', '2026-03-22 10:00:00', 'SUCCESS', 450, 'AUD-994'),
        ('RUN-005', 'Logistics_API_Pulse', '2026-03-22 10:15:00', 'SUCCESS', 11000, 'AUD-995');
    """)

    # Populate DB Audit
    cur.executescript("""
        INSERT INTO db_audit_log (audit_id, event_time, event_type, target_object, changed_by_user, user_role, access_type, environment, change_description) VALUES
        ('AUD-991', '2026-03-22 08:00:00', 'INSERT', 'fact_sales', 'etl_service', 'System', 'Write', 'PROD', 'Loaded nightly sales'),
        ('AUD-992', '2026-03-22 08:30:00', 'INSERT', 'dim_returns', 'etl_service', 'System', 'Write', 'PROD', 'Loaded nightly returns'),
        ('AUD-993', '2026-03-22 09:00:00', 'ALERT', 'dim_customers', 'etl_service', 'System', 'Write', 'PROD', 'Data truncation error on contact column'),
        ('AUD-994', '2026-03-22 10:00:00', 'UPDATE', 'fact_inventory', 'etl_sys', 'System', 'Write', 'PROD', 'Hourly stock tick'),
        ('AUD-995', '2026-03-22 10:15:00', 'INSERT', 'dim_shipments', 'api_gateway', 'Service', 'Write', 'PROD', 'API hook ingestion'),
        ('AUD-999', '2026-03-22 11:30:00', 'ALTER', 'dim_customers', 'dba_admin', 'DBA', 'DDL', 'PROD', 'Expanded varchar size for contact column');
    """)

    # Populate Access Control
    cur.executescript("""
        INSERT INTO asset_access_control (asset_name, asset_type, user_group, user_email, environment, account_type, access_level, granted_date) VALUES
        ('fact_sales', 'Target DW Table', 'Finance Team', 'finance@retail.org', 'PROD', 'Group', 'Read', '2025-01-01'),
        ('dim_customers', 'Target DW Table', 'Marketing', 'mkt@retail.org', 'PROD', 'Group', 'Read', '2025-02-15'),
        ('fact_inventory', 'Target DW Table', 'Supply Chain', 'supply@retail.org', 'PROD', 'Group', 'Read/Write', '2025-03-10'),
        ('dim_shipments', 'Target DW Table', 'Logistics', 'logistics@retail.org', 'PROD', 'Group', 'Read', '2025-04-20'),
        ('retail_orders', 'RDBMS Source', 'Data Eng', 'de@retail.org', 'PROD', 'Group', 'Admin', '2025-01-01'),
        ('customer_profiles', 'RDBMS Source', 'SecOps', 'security@retail.org', 'PROD', 'Group', 'Admin', '2024-11-01'),
        ('stripe_payments', 'API Endpoint', 'Finance Admin', 'fin_admin@retail.org', 'PROD', 'Group', 'Admin', '2025-05-05');
    """)

    # Populate BI Reports
    cur.executescript("""
        INSERT INTO report_dependency (report_name, business_owner, target_audience, dw_table, metrics_kpis, usage_frequency, run_count, last_refreshed) VALUES
        ('Daily Revenue Exec Dash', 'CFO', 'Execs', 'fact_sales', 'Total Revenue, Refunds', 'Daily', 540, '2026-03-22'),
        ('Returns Analysis', 'Support Lead', 'Support Team', 'dim_returns', 'Return Rate', 'Weekly', 120, '2026-03-22'),
        ('Customer Lifetime Value KPI', 'CMO', 'Marketing', 'dim_customers', 'LTV, Churn', 'Daily', 320, '2026-03-22'),
        ('Global Supply Chain Hub', 'COO', 'Operations Room', 'fact_inventory', 'Stockouts, Overflow', 'Hourly', 4500, '2026-03-22'),
        ('Logistics Performance Tracker', 'Logistics VP', 'Delivery Network', 'dim_shipments', 'On-time delivery %', 'Daily', 210, '2026-03-22');

        INSERT INTO bi_report_usage (report_name, user_group, user_email, access_level, run_count, last_run_timestamp, refresh_frequency) VALUES
        ('Daily Revenue Exec Dash', 'C-Suite', 'cfo@retail.org', 'Viewer', 300, '2026-03-22 09:00:00', 'Daily'),
        ('Returns Analysis', 'Support Team', 'support@retail.org', 'Editor', 120, '2026-03-21 14:00:00', 'Weekly'),
        ('Customer Lifetime Value KPI', 'Marketing Analysts', 'analyst@retail.org', 'Editor', 450, '2026-03-21 16:30:00', 'Daily'),
        ('Global Supply Chain Hub', 'Warehouse Managers', 'wh_lead@retail.org', 'Viewer', 8000, '2026-03-22 10:05:00', 'Hourly');
    """)

    conn.commit()
    conn.close()
    print("Database org_test_env.db heavily populated and successfully created.")

if __name__ == '__main__':
    create_mock_db()
