"""
setup_target_dw.py
------------------
Config-driven DW schema builder.
Reads domain from config.yaml — no code changes needed to switch domains.
"""
import sqlite3
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
from src.config_loader import get_active_domain

TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')
os.makedirs(os.path.dirname(TARGET_DB_PATH), exist_ok=True)

COMMON_TABLES = """
    DROP TABLE IF EXISTS data_lineage_map;
    DROP TABLE IF EXISTS table_catalog;
    DROP TABLE IF EXISTS report_dependency;
    DROP TABLE IF EXISTS db_audit_log;
    DROP TABLE IF EXISTS etl_execution_logs;
    DROP TABLE IF EXISTS asset_access_control;
    DROP TABLE IF EXISTS bi_report_usage;
"""

COMMON_METADATA = """
    CREATE TABLE IF NOT EXISTS data_lineage_map (
        lineage_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_system TEXT, source_table TEXT, source_column TEXT,
        target_table TEXT, target_column TEXT, transformation_logic TEXT);

    CREATE TABLE IF NOT EXISTS table_catalog (
        table_name TEXT PRIMARY KEY, description TEXT, domain TEXT,
        data_steward TEXT, etl_pipeline TEXT, refresh_frequency TEXT, pii_flag BOOLEAN);

    CREATE TABLE IF NOT EXISTS report_dependency (
        dependency_id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_name TEXT, business_owner TEXT, dw_table TEXT, usage_frequency TEXT,
        metrics_kpis TEXT, last_refreshed TIMESTAMP, run_count INTEGER DEFAULT 0);

    CREATE TABLE IF NOT EXISTS db_audit_log (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_time TIMESTAMP, event_type TEXT, target_object TEXT,
        changed_by_user TEXT, user_role TEXT, access_type TEXT,
        environment TEXT, change_description TEXT);

    CREATE TABLE IF NOT EXISTS etl_execution_logs (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        workflow_name TEXT,
        mapping_name TEXT,
        pipeline_name TEXT, source_system TEXT, target_table TEXT,
        start_time TIMESTAMP, end_time TIMESTAMP,
        records_read INTEGER, records_inserted INTEGER, records_updated INTEGER,
        transformation_metrics TEXT, status TEXT, error_message TEXT, notes TEXT,
        db_audit_ref INTEGER,
        retry_attempts INTEGER DEFAULT 0);

    CREATE TABLE IF NOT EXISTS asset_access_control (
        access_id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_name TEXT, asset_type TEXT, user_group TEXT, user_email TEXT,
        environment TEXT, account_type TEXT, access_level TEXT, granted_date TIMESTAMP);

    CREATE TABLE IF NOT EXISTS bi_report_usage (
        usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_name TEXT, user_email TEXT, user_group TEXT,
        access_level TEXT, run_count INTEGER DEFAULT 0,
        last_run_timestamp TIMESTAMP, refresh_frequency TEXT,
        metrics_kpis TEXT);
"""


def setup_healthcare(conn):
    print("Building Healthcare DW Schema...")
    c = conn.cursor()
    for t in ['Dim_Patient','Dim_Provider','Dim_Facility','Dim_Diagnosis','Dim_Medication','Dim_Date','Fact_Clinical_Encounter','Fact_Claims']:
        c.execute(f"DROP TABLE IF EXISTS {t}")

    c.execute('''CREATE TABLE Dim_Patient (patient_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_patient_id INTEGER, first_name TEXT, last_name TEXT, date_of_birth DATE,
        gender TEXT, insurance_provider TEXT, blood_type TEXT, status TEXT,
        valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)''')
    c.execute('''CREATE TABLE Dim_Provider (provider_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_provider_id INTEGER, full_name TEXT, npi_number TEXT, specialty TEXT,
        department TEXT, hospital_affiliation TEXT, status TEXT)''')
    c.execute('''CREATE TABLE Dim_Facility (facility_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_facility_id INTEGER, facility_name TEXT, facility_type TEXT,
        trauma_level TEXT, city TEXT, state TEXT)''')
    c.execute('''CREATE TABLE Dim_Diagnosis (diagnosis_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_diagnosis_id INTEGER, icd10_code TEXT, clinical_category TEXT,
        description TEXT, severity_level TEXT)''')
    c.execute('''CREATE TABLE Dim_Date (date_sk TEXT PRIMARY KEY, year INTEGER, month INTEGER,
        day INTEGER, quarter INTEGER, day_of_week TEXT, is_weekend BOOLEAN)''')
    c.execute('''CREATE TABLE Fact_Clinical_Encounter (encounter_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_sk TEXT, patient_sk INTEGER, provider_sk INTEGER, facility_sk INTEGER,
        lab_test_name TEXT, loinc_code TEXT, result_value REAL, units TEXT,
        abnormal_flag TEXT, encounter_duration_mins INTEGER)''')
    c.execute('''CREATE TABLE Fact_Claims (claim_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_claim_id TEXT, date_sk TEXT, patient_sk INTEGER, provider_sk INTEGER,
        diagnosis_sk INTEGER, claim_type TEXT, billed_amount REAL, allowed_amount REAL,
        patient_responsibility REAL, status TEXT, payer_name TEXT)''')
    conn.commit()
    print("Healthcare DW Schema Complete.")

def setup_retail(conn):
    print("Building Retail DW Schema...")
    c = conn.cursor()
    for t in ['Dim_Customer','Dim_Product','Dim_Store','Dim_Employee','Dim_Supplier','Dim_Date','Fact_Sales','Fact_Inventory']:
        c.execute(f"DROP TABLE IF EXISTS {t}")

    c.execute('''CREATE TABLE Dim_Customer (customer_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_customer_id INTEGER, first_name TEXT, last_name TEXT, email TEXT,
        customer_segment TEXT, loyalty_score INTEGER, status TEXT,
        valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)''')
    c.execute('''CREATE TABLE Dim_Product (product_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_product_id INTEGER, product_name TEXT, category TEXT, sub_category TEXT,
        price REAL, cost_price REAL, is_active BOOLEAN)''')
    c.execute('''CREATE TABLE Dim_Store (store_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        source_store_id INTEGER, store_name TEXT, store_type TEXT, city TEXT, state TEXT, region TEXT)''')
    c.execute('''CREATE TABLE Dim_Date (date_sk TEXT PRIMARY KEY, year INTEGER, month INTEGER,
        day INTEGER, quarter INTEGER, day_of_week TEXT, is_weekend BOOLEAN)''')
    c.execute('''CREATE TABLE Fact_Sales (sale_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        date_sk TEXT, customer_sk INTEGER, product_sk INTEGER, store_sk INTEGER,
        quantity INTEGER, discount_percent REAL, tax_rate REAL, payment_method TEXT, order_status TEXT)''')
    c.execute('''CREATE TABLE Fact_Inventory (inventory_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        date_sk TEXT, product_sk INTEGER, store_sk INTEGER,
        qty_on_hand INTEGER, qty_on_order INTEGER, reorder_level INTEGER, unit_cost REAL)''')
    conn.commit()
    print("Retail DW Schema Complete.")

def setup_target_dw():
    domain = get_active_domain()
    print(f"Setting up {domain.upper()} Target DW at {TARGET_DB_PATH}...")
    conn = sqlite3.connect(TARGET_DB_PATH)
    c = conn.cursor()
    # Drop and recreate shared metadata tables
    for stmt in COMMON_TABLES.strip().split(';'):
        if stmt.strip(): c.execute(stmt.strip())
    for stmt in COMMON_METADATA.strip().split(';'):
        if stmt.strip():
            try: c.execute(stmt.strip())
            except: pass
    conn.commit()

    if domain == 'healthcare':
        setup_healthcare(conn)
    elif domain == 'retail':
        setup_retail(conn)
    else:
        print(f"[WARNING] No DW schema defined for domain '{domain}'. Add setup_{domain}() to setup_target_dw.py.")
    conn.close()
    print(f"Target DW Schema Initialization Complete ({domain}).")

if __name__ == "__main__":
    setup_target_dw()
