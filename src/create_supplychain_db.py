import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "org_supplychain_env.db")

def create_supplychain_db():
    print(f"Creating org_supplychain_env.db at {DB_PATH}...")
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
        ('ERP System', 'erp_purchase_orders', 'po_number', 'fact_procurement', 'purchase_order_id', 'Direct Map'),
        ('ERP System', 'erp_purchase_orders', 'supplier_code', 'fact_procurement', 'vendor_sk', 'Lookup'),
        ('Freight API', 'global_shipping_hooks', 'container_id', 'fact_logistics', 'container_no', 'Direct Map'),
        ('Warehouse Mgmt DB', 'wms_stock_levels', 'item_sku', 'dim_inventory_master', 'product_sku', 'Direct Map'),
        ('JSON Drop', 'supplier_kpi.json', 'vendor_id', 'dim_vendors', 'vendor_id', 'JSON Parser');

        INSERT INTO table_catalog (table_name, column_name, data_type, is_pii, etl_pipeline) VALUES
        ('fact_procurement', 'purchase_order_id', 'VARCHAR', 0, 'Procurement_Nightly_Sync'),
        ('fact_logistics', 'container_no', 'VARCHAR', 0, 'Freight_Pulse_Stream'),
        ('dim_inventory_master', 'product_sku', 'VARCHAR', 0, 'Warehouse_Stock_Batch'),
        ('dim_vendors', 'vendor_id', 'VARCHAR', 0, 'Supplier_Scorecard_ETL'),
        ('erp_purchase_orders', 'po_number', 'VARCHAR', 0, 'N/A'),
        ('global_shipping_hooks', 'container_id', 'VARCHAR', 0, 'N/A'),
        ('wms_stock_levels', 'item_sku', 'VARCHAR', 0, 'N/A'),
        ('supplier_kpi.json', 'vendor_id', 'VARCHAR', 0, 'N/A');

        INSERT INTO etl_execution_logs (workflow_name, mapping_name, pipeline_name, source_system, target_table, start_time, end_time, records_read, records_inserted, records_updated, status, notes, db_audit_ref) VALUES
        ('WF_Procurement', 'MAP_PO', 'Procurement_Nightly_Sync', 'ERP System', 'fact_procurement', '2026-03-23 01:00:00', '2026-03-23 01:30:00', 450000, 450000, 0, 'SUCCESS', 'Executed full PO sync', 'AUD-S1'),
        ('WF_Freight', 'MAP_LOGISTICS', 'Freight_Pulse_Stream', 'Freight API', 'fact_logistics', '2026-03-23 02:00:00', '2026-03-23 02:05:00', 8500, 8500, 0, 'SUCCESS', 'Global freight markers plotted', 'AUD-S2'),
        ('WF_Warehouse', 'MAP_WMS', 'Warehouse_Stock_Batch', 'Warehouse Mgmt DB', 'dim_inventory_master', '2026-03-23 03:00:00', '2026-03-23 03:15:00', 1200000, 1000, 1199000, 'SUCCESS', 'Global inventory levels mutated', 'AUD-S3'),
        ('WF_Vendor', 'MAP_SCORECARD', 'Supplier_Scorecard_ETL', 'JSON Drop', 'dim_vendors', '2026-03-23 04:00:00', '2026-03-23 04:02:00', 850, 0, 0, 'FAILED', 'JSON structural integrity corrupt', 'AUD-S4');

        INSERT INTO db_audit_log (audit_id, event_time, event_type, target_object, changed_by_user, user_role, access_type, environment, change_description) VALUES
        ('AUD-S1', '2026-03-23 01:00:00', 'INSERT', 'fact_procurement', 'svc_erp', 'System', 'Write', 'PROD', 'Nightly purchase orders written'),
        ('AUD-S2', '2026-03-23 02:00:00', 'INSERT', 'fact_logistics', 'api_gateway', 'Service', 'Write', 'PROD', 'Freight APIs ingested'),
        ('AUD-S3', '2026-03-23 03:00:00', 'UPDATE', 'dim_inventory_master', 'svc_wms', 'System', 'Write', 'PROD', 'Large scale stock mutation'),
        ('AUD-S4', '2026-03-23 04:00:00', 'ALERT', 'dim_vendors', 'svc_data', 'System', 'Write', 'PROD', 'Failed to update vendor metrics');

        INSERT INTO asset_access_control (asset_name, asset_type, user_group, user_email, environment, account_type, access_level, granted_date) VALUES
        ('fact_procurement', 'Target DW Table', 'Sourcing Dept', 'sourcing@logistics.net', 'PROD', 'Group', 'Read', '2024-01-01'),
        ('fact_logistics', 'Target DW Table', 'Fleet Management', 'fleet@logistics.net', 'PROD', 'Group', 'Read/Write', '2024-02-15'),
        ('dim_inventory_master', 'Target DW Table', 'Warehouse Managers', 'wh_mgmt@logistics.net', 'PROD', 'Group', 'Admin', '2024-03-10'),
        ('erp_purchase_orders', 'RDBMS Source', 'ERP Admins', 'erp_sys@logistics.net', 'PROD', 'Group', 'Admin', '2023-11-01');

        INSERT INTO report_dependency (report_name, business_owner, target_audience, dw_table, metrics_kpis, usage_frequency, run_count, last_refreshed) VALUES
        ('Global Procurement Tracking', 'VP Procurement', 'Sourcing', 'fact_procurement', 'Spend vs Budget', 'Daily', 2100, '2026-03-23'),
        ('Active Freight Heatmap', 'Fleet Director', 'Logistics Ops', 'fact_logistics', 'On-time delivery %', 'Hourly', 5500, '2026-03-23'),
        ('Worldwide Inventory Radar', 'Inventory VP', 'Operations', 'dim_inventory_master', 'Stockouts, Overflow', 'Hourly', 9200, '2026-03-23');

        INSERT INTO bi_report_usage (report_name, user_group, user_email, access_level, run_count, last_run_timestamp, refresh_frequency) VALUES
        ('Global Procurement Tracking', 'Sourcing Analysts', 'analyst@logistics.net', 'Viewer', 1100, '2026-03-23 08:00:00', 'Daily'),
        ('Active Freight Heatmap', 'Terminal Managers', 'terminals@logistics.net', 'Editor', 2500, '2026-03-23 09:00:00', 'Hourly'),
        ('Worldwide Inventory Radar', 'Fulfillment Center', 'fc@logistics.net', 'Viewer', 4500, '2026-03-23 10:00:00', 'Hourly');
    """)

    conn.commit()
    conn.close()
    print("Database org_supplychain_env.db heavily populated and created.")

if __name__ == '__main__':
    create_supplychain_db()
