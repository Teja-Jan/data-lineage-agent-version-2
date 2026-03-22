"""
setup_access_control.py
------------------------
Config-driven RBAC / access control seeder.
Reads domain from config.yaml — no code changes to switch domains.
"""
import sqlite3, os, sys, random
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
from src.config_loader import get_active_domain, get_domain_config

TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')

DOMAIN_ACCESS = {
    'healthcare': {
        'user_groups': [
            ('Clinical Staff', 'Read-Only (PHI Unmasked)',
             ['Dim_Patient','Fact_Clinical_Encounter','Patients','Hospital Readmission Risk Dashboard','Lab_Results.csv']),
            ('HIPAA Auditors', 'Full Audit (PHI Masked)', None),  # None = all assets
            ('Billing Department', 'Read/Write (Financial)',
             ['Fact_Claims','Dim_Patient','Dim_Provider','Dim_Diagnosis','Insurance_Claims.json','Financial Claims Outcome Report']),
            ('Data Engineering', 'Admin (Structural)', None),
            ('Hospital Administration', 'Aggregate Read',
             ['Dim_Facility','Provider Performance Scorecard','Regional Outbreak Heatmap']),
        ],
        'all_assets': ['Dim_Patient','Dim_Provider','Dim_Facility','Dim_Diagnosis',
                       'Fact_Clinical_Encounter','Fact_Claims','Patients','Providers',
                       'Facilities','Diagnoses','Lab_Results.csv','Insurance_Claims.json',
                       'Hospital Readmission Risk Dashboard','Financial Claims Outcome Report',
                       'Provider Performance Scorecard','Regional Outbreak Heatmap'],
        'email_domain': 'healthsystem.org',
    },
    'retail': {
        'user_groups': [
            ('Sales Analytics', 'Read-Only',
             ['Fact_Sales','Dim_Customer','Dim_Product','Sales Performance Dashboard','Customer Segmentation Report']),
            ('Finance Team', 'Read/Write (Financial)',
             ['Fact_Sales','Margin Analysis Report','Fact_Inventory']),
            ('Data Engineering', 'Admin (Structural)', None),
            ('Supply Chain', 'Aggregate Read',
             ['Fact_Inventory','Inventory Health Report','Dim_Store']),
            ('Marketing', 'Read-Only',
             ['Dim_Customer','marketing_campaigns.json','Customer Segmentation Report']),
        ],
        'all_assets': ['Dim_Customer','Dim_Product','Dim_Store','Fact_Sales','Fact_Inventory',
                       'Customers','Products','Stores','Sales_Export.csv','marketing_campaigns.json',
                       'Sales Performance Dashboard','Customer Segmentation Report',
                       'Margin Analysis Report','Inventory Health Report'],
        'email_domain': 'retailcorp.com',
    }
}

def get_asset_type(name):
    if name.startswith('Dim_') or name.startswith('Fact_'):
        return 'Target DW Table'
    if name.endswith('.csv'):
        return 'Flat File Object'
    if name.endswith('.json'):
        return 'API Endpoint'
    if 'Dashboard' in name or 'Report' in name or 'Scorecard' in name or 'Heatmap' in name:
        return 'BI Report'
    return 'RDBMS Source Table'

def setup_access_control():
    domain = get_active_domain()
    cfg = DOMAIN_ACCESS.get(domain, DOMAIN_ACCESS['healthcare'])
    all_assets = cfg['all_assets']
    email_domain = cfg['email_domain']

    print(f"Setting up Access Control for {domain.upper()}...")
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()
    # Always wipe first so no stale values from previous runs remain
    cursor.execute("DELETE FROM asset_access_control")
    conn.commit()
    records = []

    for group_name, access_level, accessible_assets in cfg['user_groups']:
        assets_to_grant = accessible_assets if accessible_assets else all_assets
        for asset in assets_to_grant:
            for _ in range(random.randint(1, 3)):
                records.append((
                    asset, get_asset_type(asset), group_name,
                    f"user_{random.randint(1000,9999)}@{email_domain}",
                    random.choice(['Production','Production','UAT']),
                    random.choice(['Individual', 'Individual', 'Service Account']),
                    access_level,
                    (datetime.now() - timedelta(days=random.randint(10,365))).strftime('%Y-%m-%d %H:%M:%S')
                ))

    # Guarantee every asset has at least one record
    for asset in all_assets:
        records.append((asset, get_asset_type(asset), 'Data Engineering',
                        f"sysadmin_svc@{email_domain}", 'Production', 'Service Account',
                        'Admin (Structural)', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    cursor.executemany("""INSERT INTO asset_access_control
        (asset_name,asset_type,user_group,user_email,environment,account_type,access_level,granted_date)
        VALUES (?,?,?,?,?,?,?,?)""", records)
    conn.commit(); conn.close()
    print(f"Access Control seeded for {domain.upper()}.")

if __name__ == "__main__":
    setup_access_control()
