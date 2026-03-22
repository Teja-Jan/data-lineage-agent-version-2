"""
setup_audit_metadata.py
------------------------
Config-driven lineage & audit metadata seeder.
Reads domain from config.yaml — no code changes to switch domains.
"""
import sqlite3, os, sys, random
from faker import Faker
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
from src.config_loader import get_active_domain, get_domain_config

fake = Faker(); Faker.seed(42); random.seed(42)
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')

DOMAIN_METADATA = {
    'healthcare': {
        'lineage': [
            ('EMR:Epic','Patients','patient_id','Dim_Patient','source_patient_id','Type 2 SCD; Hash matching on patient_id + dob'),
            ('EMR:Epic','Patients','first_name','Dim_Patient','first_name','Trim whitespace, apply Title Case'),
            ('EMR:Epic','Patients','last_name','Dim_Patient','last_name','Trim whitespace, apply Title Case'),
            ('EMR:Epic','Patients','ssn','Dim_Patient','ssn','AES-256 Encryption at rest'),
            ('EMR:Epic','Providers','provider_id','Dim_Provider','source_provider_id','Type 1 SCD; direct replace'),
            ('EMR:Epic','Providers','npi_number','Dim_Provider','npi_number','Format validation: 10-digit NPI'),
            ('EMR:Epic','Facilities','facility_id','Dim_Facility','source_facility_id','Type 1 SCD'),
            ('EMR:Epic','Diagnoses','diagnosis_id','Dim_Diagnosis','source_diagnosis_id','Type 1 SCD'),
            ('CSV:Lab_Results','Lab_Results.csv','patient_id','Fact_Clinical_Encounter','patient_sk','SK Lookup via Dim_Patient'),
            ('CSV:Lab_Results','Lab_Results.csv','provider_id','Fact_Clinical_Encounter','provider_sk','SK Lookup via Dim_Provider'),
            ('CSV:Lab_Results','Lab_Results.csv','facility_id','Fact_Clinical_Encounter','facility_sk','SK Lookup via Dim_Facility'),
            ('CSV:Lab_Results','Lab_Results.csv','result_value','Fact_Clinical_Encounter','result_value','Cast to FLOAT, nullify if non-numeric'),
            ('CSV:Lab_Results','Lab_Results.csv','loinc_code','Fact_Clinical_Encounter','loinc_code','Validate against LOINC Master'),
            ('API:Clearinghouse','clearinghouse.io/claims','patient_id','Fact_Claims','patient_sk','SK Lookup via Dim_Patient'),
            ('API:Clearinghouse','clearinghouse.io/claims','primary_diagnosis_id','Fact_Claims','diagnosis_sk','SK Lookup via Dim_Diagnosis'),
            ('API:Clearinghouse','clearinghouse.io/claims','billed_amount','Fact_Claims','billed_amount','Direct Mapping, validate > 0'),
            ('API:Clearinghouse','clearinghouse.io/claims','payer_name','Fact_Claims','payer_name','Normalize to standard payer list'),
        ],
        'catalog': [
            ('Dim_Patient','Master patient demographic and insurance SCD Type 2','Clinical','Dr. Sarah Jennings (CMO)','EMR_TO_DW_PATIENT','Daily',True),
            ('Dim_Provider','Master list of clinical staff and specialists','Clinical','HR Compliance Officer','EMR_TO_DW_PROVIDER','Daily',False),
            ('Dim_Facility','Hospital, clinic, and rehab center locations','Operations','Facilities Admin','EMR_TO_DW_FACILITY','Weekly',False),
            ('Dim_Diagnosis','Master ICD-10 diagnostic dictionary','Metadata','Clinical Informatics','EMR_TO_DW_DIAGNOSIS','Monthly',False),
            ('Fact_Clinical_Encounter','Lab test transactions and clinical encounters','Clinical','Quality Assurance Director','FLATFILE_TO_DW_ENCOUNTER','Daily',True),
            ('Fact_Claims','Financial billed, allowed, and patient responsibility data','Financial','CFO Office','API_TO_DW_CLAIMS','Daily',True),
            ('Patients','Source EMR Patient Master Records','Clinical EMR','EMR Admin','N/A','Real-time',True),
            ('Providers','Source EMR Medical Staff Directory','Clinical EMR','EMR Admin','N/A','Real-time',False),
            ('Facilities','Source EMR Hospital Site Records','Clinical EMR','EMR Admin','N/A','Real-time',False),
            ('Diagnoses','Source EMR ICD-10 Reference Table','Clinical EMR','EMR Admin','N/A','Real-time',False),
            ('Lab_Results.csv','LIS system bulk flat file export','Flat File','Lab Supervisor','N/A','Batch',True),
            ('clearinghouse.io/claims','Insurance claims clearinghouse REST endpoint','API','Billing Director','N/A','Streaming',True),
        ],
        'reports': [
            # (report_name, owner, dw_table, frequency, metrics_kpis)
            ('Hospital Readmission Risk Dashboard','Chief Medical Officer','Dim_Patient','Daily',
             'Readmission Rate (%), Avg Length of Stay, High-Risk Patient Count, 30-Day Readmission Score'),
            ('Hospital Readmission Risk Dashboard','Chief Medical Officer','Fact_Clinical_Encounter','Daily',
             'Encounter Volume, Lab Abnormal Rate (%), Avg Encounter Duration (mins)'),
            ('Hospital Readmission Risk Dashboard','Chief Medical Officer','Dim_Diagnosis','Daily',
             'Top 10 Diagnoses by Volume, Severity Distribution, ICD-10 Frequency'),
            ('Financial Claims Outcome Report','CFO','Fact_Claims','Daily',
             'Total Billed ($), Total Allowed ($), Denial Rate (%), Net Collection Rate (%)'),
            ('Financial Claims Outcome Report','CFO','Dim_Provider','Daily',
             'Revenue per Provider, Claims per Specialty, Avg Patient Responsibility ($)'),
            ('Provider Performance Scorecard','Quality Assurance Director','Fact_Clinical_Encounter','Weekly',
             'Patient Satisfaction Score, Encounter Throughput, Avg Duration per Case'),
            ('Provider Performance Scorecard','Quality Assurance Director','Fact_Claims','Weekly',
             'Revenue Generated per Provider, Denial Count per Provider'),
            ('Regional Outbreak Heatmap','Public Health Director','Dim_Facility','Daily',
             'Geographic Case Density, Infection Rate by Facility Type, Surge Risk Index'),
            ('Regional Outbreak Heatmap','Public Health Director','Dim_Diagnosis','Daily',
             'Outbreak Category Trend, ICD-10 Cluster Analysis, 7-Day Rolling Average'),
        ],
        'audit_users': [
            ('S.Jennings','Chief Medical Officer','Read'),
            ('M.Smith','Billing Admin','Update'),
            ('A.Patel','Data Engineer','Alter'),
            ('J.Doe','ER Nurse Supervisor','Read'),
            ('sys_admin','DBA','Grant'),
            ('R.Williams','HIPAA Compliance Officer','Read'),
            ('K.Brooks','Clinical Analyst','Read'),
        ],
        'audit_tables': [
            'Dim_Patient','Dim_Provider','Dim_Facility','Dim_Diagnosis',
            'Fact_Clinical_Encounter','Fact_Claims','Lab_Results.csv','clearinghouse.io/claims',
            'Patients','Providers','Facilities','Diagnoses',
        ],
        'bi_users': [
            ('S.Jennings@healthsystem.org','Hospital Administration','Read-Only (Aggregate)'),
            ('C.Thompson@healthsystem.org','Clinical Staff','Read-Only (PHI Unmasked)'),
            ('M.Smith@healthsystem.org','Billing Department','Read/Write (Financial)'),
            ('R.Williams@healthsystem.org','HIPAA Auditors','Full Audit (PHI Masked)'),
            ('K.Brooks@healthsystem.org','Data Engineering','Admin (Structural)'),
        ],
        'bi_reports_list': [
            'Hospital Readmission Risk Dashboard',
            'Financial Claims Outcome Report',
            'Provider Performance Scorecard',
            'Regional Outbreak Heatmap',
        ],
    },
    'retail': {
        'lineage': [
            ('OLTP','Customers','customer_id','Dim_Customer','source_customer_id','Type 2 SCD'),
            ('OLTP','Customers','first_name','Dim_Customer','first_name','Trim, Title Case'),
            ('OLTP','Products','product_id','Dim_Product','source_product_id','Type 1 SCD'),
            ('OLTP','Stores','store_id','Dim_Store','source_store_id','Type 1 SCD'),
            ('CSV:Sales','Sales_Export.csv','customer_id','Fact_Sales','customer_sk','SK Lookup via Dim_Customer'),
            ('CSV:Sales','Sales_Export.csv','product_id','Fact_Sales','product_sk','SK Lookup via Dim_Product'),
            ('CSV:Sales','Sales_Export.csv','quantity','Fact_Sales','quantity','Direct Mapping'),
            ('API:Marketing','marketingcloud.io/campaigns','campaign_id','Dim_Campaign','source_campaign_id','Type 1 SCD'),
        ],
        'catalog': [
            ('Dim_Customer','Master customer dimension with loyalty data','Retail','Head of CRM','OLTP_TO_DW_CUSTOMER','Daily',True),
            ('Dim_Product','Master product catalog with pricing','Retail','Product Manager','OLTP_TO_DW_PRODUCT','Daily',False),
            ('Dim_Store','Store location and region dimension','Operations','Store Ops','OLTP_TO_DW_STORE','Weekly',False),
            ('Fact_Sales','Sales transaction fact table','Retail','Sales Analytics','FLATFILE_TO_DW_SALES','Daily',False),
            ('Fact_Inventory','Inventory snapshot facts','Supply Chain','Inventory Manager','FLATFILE_TO_DW_INVENTORY','Daily',False),
            ('Customers','Source OLTP Customer Records','OLTP','DB Admin','N/A','Real-time',True),
            ('Products','Source OLTP Product Records','OLTP','DB Admin','N/A','Real-time',False),
            ('Stores','Source OLTP Store Records','OLTP','DB Admin','N/A','Real-time',False),
            ('Sales_Export.csv','POS flat file export','Flat File','Sales Ops','N/A','Batch',False),
            ('marketingcloud.io/campaigns','Marketing cloud REST API endpoint','API','Marketing','N/A','Streaming',False),
        ],
        'reports': [
            ('Sales Performance Dashboard','VP Sales','Fact_Sales','Daily',
             'Total Revenue ($), Units Sold, Avg Order Value ($), Revenue by Region'),
            ('Sales Performance Dashboard','VP Sales','Dim_Product','Daily',
             'Top 20 Products by Revenue, Category Mix, Margin %'),
            ('Customer Segmentation Report','Head of CRM','Dim_Customer','Weekly',
             'Customer Lifetime Value, Churn Risk Score, Segment Distribution'),
            ('Margin Analysis Report','CFO','Fact_Sales','Monthly',
             'Gross Margin (%), COGS, Net Profit by Category'),
            ('Inventory Health Report','Supply Chain','Fact_Inventory','Daily',
             'Days of Supply, Out-of-Stock Rate (%), Reorder Alerts Count'),
        ],
        'audit_users': [
            ('J.Smith','VP Sales','Read'),('L.Chen','Data Engineer','Alter'),
            ('A.Brown','Analyst','Read'),('R.Patel','DBA','Grant'),('sys_admin','DBA','Grant'),
        ],
        'audit_tables': [
            'Dim_Customer','Dim_Product','Dim_Store','Fact_Sales','Fact_Inventory',
            'Customers','Products','Sales_Export.csv','marketingcloud.io/campaigns',
        ],
        'bi_users': [
            ('j.smith@retailcorp.com','Sales Analytics','Read-Only'),
            ('l.chen@retailcorp.com','Data Engineering','Admin (Structural)'),
            ('a.brown@retailcorp.com','Finance Team','Read/Write (Financial)'),
        ],
        'bi_reports_list': [
            'Sales Performance Dashboard',
            'Customer Segmentation Report',
            'Margin Analysis Report',
            'Inventory Health Report',
        ],
    }
}


def setup_audit_metadata():
    domain = get_active_domain()
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()
    meta = DOMAIN_METADATA.get(domain, DOMAIN_METADATA['healthcare'])

    print(f"Seeding {domain.upper()} Lineage Map...")
    cursor.executemany(
        "INSERT INTO data_lineage_map (source_system,source_table,source_column,target_table,target_column,transformation_logic) VALUES (?,?,?,?,?,?)",
        meta['lineage'])

    print(f"Seeding {domain.upper()} Table Catalog...")
    cursor.executemany(
        "INSERT OR REPLACE INTO table_catalog (table_name,description,domain,data_steward,etl_pipeline,refresh_frequency,pii_flag) VALUES (?,?,?,?,?,?,?)",
        meta['catalog'])

    print(f"Seeding {domain.upper()} Report Dependencies with Metrics/KPIs...")
    now = datetime.now()
    report_rows = []
    for r in meta['reports']:
        report_name, owner, dw_table, frequency, metrics = r
        last_refresh = now - timedelta(hours=random.randint(1, 48))
        run_count = random.randint(10, 500)
        report_rows.append((report_name, owner, dw_table, frequency, metrics, last_refresh.strftime('%Y-%m-%d %H:%M:%S'), run_count))
    cursor.executemany(
        "INSERT INTO report_dependency (report_name,business_owner,dw_table,usage_frequency,metrics_kpis,last_refreshed,run_count) VALUES (?,?,?,?,?,?,?)",
        report_rows)

    print(f"Seeding BI Report Usage (Users, Access Levels, Run Counts)...")
    bi_usage_rows = []
    for report_name in meta['bi_reports_list']:
        for user_email, user_group, access_level in meta['bi_users']:
            run_count = random.randint(1, 150)
            last_run = datetime.now() - timedelta(hours=random.randint(1, 72))
            freq = random.choice(['Daily', 'Weekly', 'On-Demand'])
            # Find metrics for this report from reports list
            kpis = next((r[4] for r in meta['reports'] if r[0] == report_name), 'N/A')
            bi_usage_rows.append((
                report_name, user_email, user_group, access_level,
                run_count, last_run.strftime('%Y-%m-%d %H:%M:%S'), freq, kpis
            ))
    cursor.executemany(
        "INSERT INTO bi_report_usage (report_name,user_email,user_group,access_level,run_count,last_run_timestamp,refresh_frequency,metrics_kpis) VALUES (?,?,?,?,?,?,?,?)",
        bi_usage_rows)

    print(f"Seeding Historical DB Audit Trail for {domain.upper()} (5000 entries)...")
    audit_logs = []
    start_date = datetime.now() - timedelta(days=180)
    events = ['SELECT','UPDATE','INSERT','DELETE','ALTER TABLE','GRANT ACCESS','REVOKE ACCESS']
    for i in range(5000):
        log_date = start_date + timedelta(minutes=random.randint(0, 180*24*60))
        user = random.choice(meta['audit_users'])
        evt = random.choice(events)
        tbl = random.choice(meta['audit_tables'])
        if domain == 'healthcare':
            desc = f"HIPAA compliance: {evt} on {tbl} — session tracked"
        else:
            desc = f"Routine operation: {evt} on {tbl}"
        audit_logs.append((
            log_date.strftime('%Y-%m-%d %H:%M:%S'), evt, tbl,
            user[0], user[1], user[2],
            random.choice(['PRD','PRD','PRD','UAT','DEV']), desc
        ))
    cursor.executemany(
        "INSERT INTO db_audit_log (event_time,event_type,target_object,changed_by_user,user_role,access_type,environment,change_description) VALUES (?,?,?,?,?,?,?,?)",
        audit_logs)

    conn.commit()
    conn.close()
    print("Audit & Metadata Seeding Complete.")

if __name__ == "__main__":
    setup_audit_metadata()
