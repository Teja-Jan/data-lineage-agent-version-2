import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "org_finance_env.db")

def create_finance_db():
    print(f"Creating org_finance_env.db at {DB_PATH}...")
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

    # Populate Lineage Map (Finance System)
    cur.executescript("""
        INSERT INTO data_lineage_map (source_system, source_table, source_column, target_table, target_column, transformation_logic) VALUES
        ('Oracle DB', 'banking_transactions', 'txn_id', 'fact_ledger', 'transaction_id', 'Direct Map'),
        ('Oracle DB', 'banking_transactions', 'amount', 'fact_ledger', 'ledger_value', 'CAST(Decimal)'),
        ('Auth API', 'user_accounts', 'account_hash', 'dim_accounts', 'account_id', 'SHA256 Hash'),
        ('CSV Export', 'fraud_flags.csv', 'flag_code', 'dim_fraud', 'alert_code', 'Direct Map');
    """)

    # Populate Catalog
    cur.executescript("""
        INSERT INTO table_catalog (table_name, column_name, data_type, is_pii, etl_pipeline) VALUES
        ('fact_ledger', 'transaction_id', 'VARCHAR', 0, 'Finance_Nightly_Batch'),
        ('dim_accounts', 'account_id', 'VARCHAR', 1, 'Account_Sync_API'),
        ('banking_transactions', 'txn_id', 'VARCHAR', 0, 'N/A'),
        ('user_accounts', 'account_hash', 'VARCHAR', 1, 'N/A'),
        ('fraud_flags.csv', 'flag_code', 'INT', 0, 'N/A');
    """)

    # Populate ETL Logs
    cur.executescript("""
        INSERT INTO etl_execution_logs (run_id, pipeline_name, start_time, status, records_processed, audit_ref) VALUES
        ('BATCH-F1', 'Finance_Nightly_Batch', '2026-03-23 02:00:00', 'SUCCESS', 2500000, 'AUD-F100'),
        ('BATCH-F2', 'Account_Sync_API', '2026-03-23 02:15:00', 'SUCCESS', 84000, 'AUD-F101');
    """)

    # Populate DB Audit
    cur.executescript("""
        INSERT INTO db_audit_log (audit_id, event_time, event_type, target_object, changed_by_user, user_role, access_type, environment, change_description) VALUES
        ('AUD-F100', '2026-03-23 02:00:00', 'UPDATE', 'fact_ledger', 'etl_finance_svc', 'System', 'Write', 'PROD', 'Loaded daily transactions'),
        ('AUD-F102', '2026-03-23 09:30:00', 'SELECT', 'dim_accounts', 'auditor_jdoe', 'Auditor', 'Read', 'PROD', 'Compliance review sweep');
    """)

    # Populate Access Control
    cur.executescript("""
        INSERT INTO asset_access_control (asset_name, asset_type, user_group, user_email, environment, account_type, access_level, granted_date) VALUES
        ('fact_ledger', 'DW', 'Risk Commitee', 'risk@finance.org', 'PROD', 'Group', 'Read', '2024-06-01'),
        ('banking_transactions', 'Source', 'DBA Team', 'dba@finance.org', 'PROD', 'Group', 'Admin', '2023-01-01');
    """)

    # Populate BI Reports
    cur.executescript("""
        INSERT INTO report_dependency (report_name, business_owner, target_audience, dw_table, metrics_kpis, usage_frequency, run_count, last_refreshed) VALUES
        ('Daily Trading Volume', 'Chief Risk Officer', 'Executive Board', 'fact_ledger', 'Gross Trade Volume', 'Daily', 1850, '2026-03-23'),
        ('Fraud Detection Tracker', 'Fraud Lead', 'Compliance Team', 'dim_fraud', 'High Risk Txns', 'Hourly', 4500, '2026-03-23');

        INSERT INTO bi_report_usage (report_name, user_group, user_email, access_level, run_count, last_run_timestamp, refresh_frequency) VALUES
        ('Daily Trading Volume', 'Execs', 'ceo@finance.org', 'Viewer', 850, '2026-03-23 08:00:00', 'Daily'),
        ('Fraud Detection Tracker', 'Analysts', 'analyst@finance.org', 'Editor', 4000, '2026-03-23 09:15:00', 'Hourly');
    """)

    conn.commit()
    conn.close()
    print("Database org_finance_env.db successfully created.")

if __name__ == '__main__':
    create_finance_db()
