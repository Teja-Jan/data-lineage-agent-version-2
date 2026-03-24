import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "org_insurance_env.db")

def create_insurance_db():
    print(f"Creating org_insurance_env.db at {DB_PATH}...")
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
        ('Policy Admin System', 'active_policies', 'policy_id', 'fact_policies', 'policy_sk', 'Surrogate Key'),
        ('Policy Admin System', 'active_policies', 'premium_amt', 'fact_policies', 'premium_revenue', 'Numeric Cast'),
        ('Claims Gateway API', 'submitted_claims', 'claim_hash', 'fact_claims_history', 'claim_id', 'Hash Decoder'),
        ('Broker DB', 'broker_roster', 'broker_code', 'dim_brokers', 'broker_id', 'Direct Map'),
        ('CSV Files', 'actuarial_risk_models.csv', 'risk_score', 'dim_risk_factors', 'base_score', 'Float Cast');

        INSERT INTO table_catalog (table_name, column_name, data_type, is_pii, etl_pipeline) VALUES
        ('fact_policies', 'policy_sk', 'INT', 1, 'Policy_Master_Sync'),
        ('fact_policies', 'premium_revenue', 'DECIMAL', 0, 'Policy_Master_Sync'),
        ('fact_claims_history', 'claim_id', 'VARCHAR', 1, 'Claims_Ingestion_Stream'),
        ('dim_brokers', 'broker_id', 'VARCHAR', 1, 'Broker_Sync_Nightly'),
        ('dim_risk_factors', 'base_score', 'DECIMAL', 0, 'Risk_Modeling_Batch'),
        ('active_policies', 'policy_id', 'VARCHAR', 1, 'N/A'),
        ('submitted_claims', 'claim_hash', 'VARCHAR', 1, 'N/A'),
        ('broker_roster', 'broker_code', 'VARCHAR', 0, 'N/A'),
        ('actuarial_risk_models.csv', 'risk_score', 'DECIMAL', 0, 'N/A');

        INSERT INTO etl_execution_logs (workflow_name, mapping_name, pipeline_name, source_system, target_table, start_time, end_time, records_read, records_inserted, records_updated, status, notes, db_audit_ref) VALUES
        ('WF_Policy', 'MAP_POLICIES', 'Policy_Master_Sync', 'Policy Admin System', 'fact_policies', '2026-03-23 01:00:00', '2026-03-23 02:00:00', 8500000, 850000, 7650000, 'SUCCESS', 'Executed full policy synchronization', 'AUD-I1'),
        ('WF_Claims', 'MAP_CLAIMS', 'Claims_Ingestion_Stream', 'Claims Gateway API', 'fact_claims_history', '2026-03-23 02:30:00', '2026-03-23 02:45:00', 12000, 12000, 0, 'SUCCESS', 'Realtime claims queue cleared', 'AUD-I2'),
        ('WF_Broker', 'MAP_BROKERS', 'Broker_Sync_Nightly', 'Broker DB', 'dim_brokers', '2026-03-23 03:00:00', '2026-03-23 03:10:00', 4500, 50, 4450, 'SUCCESS', 'Broker updates applied', 'AUD-I3'),
        ('WF_Risk', 'MAP_MODELS', 'Risk_Modeling_Batch', 'CSV Files', 'dim_risk_factors', '2026-03-23 04:00:00', '2026-03-23 04:05:00', 120, 0, 0, 'FAILED', 'Actuarial coefficient format error', 'AUD-I4');

        INSERT INTO db_audit_log (audit_id, event_time, event_type, target_object, changed_by_user, user_role, access_type, environment, change_description) VALUES
        ('AUD-I1', '2026-03-23 01:00:00', 'UPDATE', 'fact_policies', 'svc_policy', 'System', 'Write', 'PROD', 'Bulk policy updates'),
        ('AUD-I2', '2026-03-23 02:30:00', 'INSERT', 'fact_claims_history', 'api_gateway', 'Service', 'Write', 'PROD', 'Claims hooks received'),
        ('AUD-I3', '2026-03-23 03:00:00', 'UPDATE', 'dim_brokers', 'svc_broker', 'System', 'Write', 'PROD', 'Broker performance updates'),
        ('AUD-I4', '2026-03-23 04:00:00', 'ALERT', 'dim_risk_factors', 'svc_actuary', 'System', 'Write', 'PROD', 'Python actuarial script crashed');

        INSERT INTO asset_access_control (asset_name, asset_type, user_group, user_email, environment, account_type, access_level, granted_date) VALUES
        ('fact_policies', 'Target DW Table', 'Underwriting', 'underwrite@insure.org', 'PROD', 'Group', 'Read/Write', '2024-01-01'),
        ('fact_claims_history', 'Target DW Table', 'Claims Adjusters', 'adjusters@insure.org', 'PROD', 'Group', 'Read', '2024-02-15'),
        ('dim_risk_factors', 'Target DW Table', 'Actuaries', 'actuary@insure.org', 'PROD', 'Group', 'Admin', '2024-03-10'),
        ('active_policies', 'RDBMS Source', 'Core IT', 'db_admin@insure.org', 'PROD', 'Group', 'Admin', '2023-11-01');

        INSERT INTO report_dependency (report_name, business_owner, target_audience, dw_table, metrics_kpis, usage_frequency, run_count, last_refreshed) VALUES
        ('Global Policy Dashboard', 'Chief Underwriter', 'Execs', 'fact_policies', 'Total Active Premiums', 'Daily', 3400, '2026-03-23'),
        ('Claim Resolution Tracker', 'VP Claims', 'Adjusters', 'fact_claims_history', 'Payout Amounts, Denials', 'Hourly', 8500, '2026-03-23'),
        ('Broker Commissions Report', 'Sales Director', 'Broker Network', 'dim_brokers', 'Commission Payouts', 'Weekly', 1200, '2026-03-23');

        INSERT INTO bi_report_usage (report_name, user_group, user_email, access_level, run_count, last_run_timestamp, refresh_frequency) VALUES
        ('Global Policy Dashboard', 'C-Suite', 'ceo@insure.org', 'Viewer', 1500, '2026-03-23 08:00:00', 'Daily'),
        ('Claim Resolution Tracker', 'Regional Managers', 'rm@insure.org', 'Editor', 4000, '2026-03-23 09:00:00', 'Hourly'),
        ('Broker Commissions Report', 'Finance', 'finance@insure.org', 'Viewer', 800, '2026-03-23 10:00:00', 'Weekly');
    """)

    conn.commit()
    conn.close()
    print("Database org_insurance_env.db heavily populated and created.")

if __name__ == '__main__':
    create_insurance_db()
