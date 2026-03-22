"""
generate_sources.py
-------------------
Config-driven source data generator.
Reads domain from config.yaml — no hardcoding.
Switch domains by changing config.yaml only.
"""
import sqlite3
import pandas as pd
import json
import os
import sys
import random
from faker import Faker
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
from src.config_loader import get_active_domain

fake = Faker()
Faker.seed(42)
random.seed(42)

DB_PATH = os.path.join(BASE_DIR, 'data', 'source_rdbms', 'source_system.db')
FLAT_FILE_DIR = os.path.join(BASE_DIR, 'data', 'source_flatfiles')
API_MOCK_DIR  = os.path.join(BASE_DIR, 'data', 'source_api')

# ============================================================
# HEALTHCARE GENERATORS
# ============================================================
def generate_healthcare():
    print("Generating Clinical EMR Source (Healthcare)...")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for t in ['Patients','Providers','Facilities','Diagnoses','Medications']:
        c.execute(f"DROP TABLE IF EXISTS {t}")

    c.execute('''CREATE TABLE Patients (patient_id INT PRIMARY KEY, first_name TEXT, last_name TEXT,
        date_of_birth DATE, gender TEXT, address TEXT, phone_number TEXT, ssn TEXT,
        insurance_provider TEXT, emergency_contact TEXT, blood_type TEXT,
        registration_date DATE, last_encounter_date TIMESTAMP, status TEXT, notes TEXT)''')
    c.execute('''CREATE TABLE Providers (provider_id INT PRIMARY KEY, first_name TEXT, last_name TEXT,
        npi_number TEXT, specialty TEXT, department TEXT, hospital_affiliation TEXT, hire_date DATE, status TEXT)''')
    c.execute('''CREATE TABLE Facilities (facility_id INT PRIMARY KEY, facility_name TEXT, facility_type TEXT,
        beds INT, trauma_level TEXT, city TEXT, state TEXT, zip_code TEXT)''')
    c.execute('''CREATE TABLE Diagnoses (diagnosis_id INT PRIMARY KEY, icd10_code TEXT, description TEXT,
        clinical_category TEXT, severity_level TEXT)''')
    c.execute('''CREATE TABLE Medications (medication_id INT PRIMARY KEY, ndc_code TEXT, generic_name TEXT,
        brand_name TEXT, drug_class TEXT, dosage_form TEXT)''')

    patients = [(100000+i, fake.first_name(), fake.last_name(),
        fake.date_of_birth(minimum_age=1,maximum_age=90).strftime('%Y-%m-%d'),
        random.choice(['M','F','Other']), fake.address().replace('\n',', '), fake.phone_number(),
        fake.ssn(), random.choice(['Medicare','Medicaid','BlueCross','Aetna','Cigna','Uninsured']),
        fake.name(), random.choice(['A+','A-','B+','B-','O+','O-','AB+','AB-']),
        fake.date_between(start_date='-5y',end_date='today').strftime('%Y-%m-%d'),
        fake.date_time_between(start_date='-30d',end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        random.choice(['Active','Discharged','Deceased']),
        fake.sentence() if random.random()>0.8 else None) for i in range(3000)]
    c.executemany('INSERT INTO Patients VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', patients)

    providers = [(1000+i, fake.first_name(), fake.last_name(),
        str(fake.random_number(digits=10,fix_len=True)),
        random.choice(['Cardiology','Neurology','Oncology','Pediatrics','General Practice','Emergency Medicine']),
        random.choice(['Surgery','Internal Medicine','ER','ICU']),
        fake.company()+" Health System",
        fake.date_between(start_date='-15y',end_date='-1y').strftime('%Y-%m-%d'),
        random.choice(['Active','On Leave','Retired'])) for i in range(100)]
    c.executemany('INSERT INTO Providers VALUES (?,?,?,?,?,?,?,?,?)', providers)

    facilities = [(100+i, f"{fake.city()} Medical Center",
        random.choice(['General Hospital','Specialty Clinic','Urgent Care','Rehabilitation']),
        random.randint(50,1000), random.choice(['Level I','Level II','Level III','None']),
        fake.city(), fake.state_abbr(), fake.zipcode()) for i in range(20)]
    c.executemany('INSERT INTO Facilities VALUES (?,?,?,?,?,?,?,?)', facilities)

    diags = [(500+i, d[0], d[1], d[2], d[3]) for i,d in enumerate([
        ('E11.9','Type 2 diabetes mellitus','Endocrine','Moderate'),
        ('I10','Essential (primary) hypertension','Cardiovascular','Low'),
        ('J18.9','Pneumonia, unspecified organism','Respiratory','High'),
        ('C34.90','Malignant neoplasm of bronchus','Oncology','Critical'),
        ('F32.9','Major depressive disorder','Psychiatric','Moderate')])]
    c.executemany('INSERT INTO Diagnoses VALUES (?,?,?,?,?)', diags)

    meds = [(200+i, m[0], m[1], m[2], m[3], m[4]) for i,m in enumerate([
        ('0049-0050-50','Atorvastatin','Lipitor','Statin','Tablet'),
        ('0069-3150-83','Lisinopril','Prinivil','ACE Inhibitor','Tablet'),
        ('65862-598-05','Metformin','Glucophage','Antidiabetic','Tablet'),
        ('0088-2220-33','Albuterol','ProAir','Bronchodilator','Inhaler'),
        ('0173-0870-10','Fluticasone','Flonase','Corticosteroid','Nasal Spray')])]
    c.executemany('INSERT INTO Medications VALUES (?,?,?,?,?,?)', meds)
    conn.commit(); conn.close()

    # Lab Results flat file
    p_ids=[p[0] for p in patients]; prov_ids=[p[0] for p in providers]; f_ids=[f[0] for f in facilities]
    tests=[('Complete Blood Count','58410-5','cells/mcL'),('Lipid Panel','24331-1','mg/dL'),
           ('Hemoglobin A1C','4548-4','%'),('TSH','3016-3','mIU/L')]
    os.makedirs(FLAT_FILE_DIR, exist_ok=True)
    labs=[{'lab_id':f"LAB-{1000000+i}",'patient_id':random.choice(p_ids),
           'provider_id':random.choice(prov_ids),'facility_id':random.choice(f_ids),
           'test_name':t[0],'loinc_code':t[1],'result_value':round(random.uniform(1,500),2),
           'units':t[2],'abnormal_flag':random.choice(['Normal','High','Low']),
           'order_timestamp':(datetime.now()-timedelta(days=random.randint(0,730))).strftime('%Y-%m-%d %H:%M:%S'),
           'result_timestamp':(datetime.now()-timedelta(days=random.randint(0,700))).strftime('%Y-%m-%d %H:%M:%S')}
          for i in range(20000) for t in [random.choice(tests)]]
    pd.DataFrame(labs).to_csv(os.path.join(FLAT_FILE_DIR,'Lab_Results.csv'),index=False)

    # Insurance Claims API mock
    d_ids=[d[0] for d in diags]
    claims=[{'claim_id':f"CLM-{50000+i}",'patient_id':random.choice(p_ids),
             'primary_diagnosis_id':random.choice(d_ids),'attending_provider_id':random.choice(prov_ids),
             'claim_type':random.choice(['Professional','Institutional','Dental','Pharmacy']),
             'billed_amount':round(random.uniform(100,50000),2),
             'allowed_amount':round(random.uniform(50,40000),2),
             'patient_responsibility':round(random.uniform(0,5000),2),
             'status':random.choice(['Paid','Denied','Pending','Appealed']),
             'submission_date':(datetime.now()-timedelta(days=random.randint(0,365))).strftime('%Y-%m-%d'),
             'payer_name':random.choice(['Medicare','BlueShield','Cigna','Aetna'])} for i in range(500)]
    os.makedirs(API_MOCK_DIR, exist_ok=True)
    with open(os.path.join(API_MOCK_DIR,'Insurance_Claims.json'),'w') as f:
        json.dump({'data':claims,'metadata':{'total_records':len(claims)}},f,indent=4)
    print("Healthcare Source Generation Complete.")
    return [p[0] for p in patients], [p[0] for p in providers], [f[0] for f in facilities], [d[0] for d in diags]


# ============================================================
# RETAIL GENERATORS
# ============================================================
def generate_retail():
    print("Generating OLTP Source (Retail)...")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for t in ['Customers','Products','Stores','Employees','Suppliers']:
        c.execute(f"DROP TABLE IF EXISTS {t}")

    c.execute('''CREATE TABLE Customers (customer_id INT PRIMARY KEY, first_name TEXT, last_name TEXT,
        email TEXT, phone_number TEXT, address TEXT, city TEXT, state TEXT, country TEXT, zip_code TEXT,
        registration_date DATE, last_login_timestamp TIMESTAMP, loyalty_score INT,
        customer_segment TEXT, is_newsletter_subscriber BOOLEAN, preferred_language TEXT, system_status TEXT, notes TEXT)''')
    c.execute('''CREATE TABLE Products (product_id INT PRIMARY KEY, product_name TEXT, category TEXT,
        sub_category TEXT, price REAL, cost_price REAL, supplier_id INT, stock_quantity INT,
        warehouse_location TEXT, weight_kg REAL, dimensions TEXT, is_active BOOLEAN, launch_date DATE, description TEXT)''')
    c.execute('''CREATE TABLE Stores (store_id INT PRIMARY KEY, store_name TEXT, store_type TEXT,
        floor_space_sqft INT, open_date DATE, manager_name TEXT, city TEXT, state TEXT, country TEXT, postal_code TEXT, region TEXT)''')
    c.execute('''CREATE TABLE Employees (employee_id INT PRIMARY KEY, first_name TEXT, last_name TEXT,
        job_title TEXT, department TEXT, hire_date DATE, store_id INT)''')
    c.execute('''CREATE TABLE Suppliers (supplier_id INT PRIMARY KEY, supplier_name TEXT, contact_name TEXT,
        contact_email TEXT, country TEXT, rating INT)''')

    customers = [(10000+i, fake.first_name(), fake.last_name(), fake.ascii_email(), fake.phone_number(),
        fake.street_address().replace('\n',', '), fake.city(), fake.state_abbr(), 'USA', fake.zipcode(),
        fake.date_between(start_date='-5y',end_date='today').strftime('%Y-%m-%d'),
        fake.date_time_between(start_date='-30d',end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        random.randint(0,1000), random.choice(['Gold','Silver','Bronze','Platinum']),
        random.choice([0,1]), random.choice(['en','es','fr','de']),
        random.choice(['Active','Inactive','Suspended']),
        fake.sentence() if random.random()>0.5 else None) for i in range(3000)]
    c.executemany('INSERT INTO Customers VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', customers)

    products = [(20000+i, fake.catch_phrase(), random.choice(['Electronics','Clothing','Home','Toys','Books']),
        random.choice(['Mobile','Kitchen','Apparel','Educational','Fiction']),
        round(random.uniform(10,500),2), round(random.uniform(5,250),2),
        fake.random_int(min=1,max=50), random.randint(0,1000),
        f"WH-{random.choice(['A','B','C'])}-{random.randint(1,100)}",
        round(random.uniform(0.1,20),2), f"{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(5,50)}",
        random.choice([0,1]), fake.date_between(start_date='-10y',end_date='-1y').strftime('%Y-%m-%d'),
        fake.text(max_nb_chars=100)) for i in range(600)]
    c.executemany('INSERT INTO Products VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', products)

    stores = [(100+i, f"{fake.city()} Superstore", random.choice(['Retail','Outlet','Flagship']),
        random.randint(5000,50000), fake.date_between(start_date='-20y',end_date='-1y').strftime('%Y-%m-%d'),
        fake.name(), fake.city(), fake.state_abbr(), 'USA', fake.zipcode(),
        random.choice(['North','South','East','West','Central'])) for i in range(50)]
    c.executemany('INSERT INTO Stores VALUES (?,?,?,?,?,?,?,?,?,?,?)', stores)

    store_ids = [s[0] for s in stores]
    employees = [(3000+i, fake.first_name(), fake.last_name(),
        random.choice(['Cashier','Manager','Stock Clerk','Customer Service']),
        random.choice(['Sales','Operations','Management']),
        fake.date_between(start_date='-10y',end_date='now').strftime('%Y-%m-%d'),
        random.choice(store_ids)) for i in range(500)]
    c.executemany('INSERT INTO Employees VALUES (?,?,?,?,?,?,?)', employees)

    suppliers = [(1+i, fake.company(), fake.name(), fake.company_email(),
        fake.country(), random.randint(1,100)) for i in range(50)]
    c.executemany('INSERT INTO Suppliers VALUES (?,?,?,?,?,?)', suppliers)
    conn.commit(); conn.close()

    # Sales flat file
    c_ids=[c[0] for c in customers]; p_ids=[p[0] for p in products]; s_ids=[s[0] for s in stores]; e_ids=[e[0] for e in employees]
    os.makedirs(FLAT_FILE_DIR, exist_ok=True)
    orders=[{'transaction_id':f"TXN-{100000+i}",'customer_id':random.choice(c_ids),
             'product_id':random.choice(p_ids),'quantity':random.randint(1,15),
             'discount_percent':round(random.uniform(0,0.3),2),'tax_rate':0.08,
             'order_timestamp':(datetime.now()-timedelta(days=random.randint(0,730))).strftime('%Y-%m-%d %H:%M:%S'),
             'payment_method':random.choice(['Credit Card','PayPal','Bank Transfer']),
             'store_id':random.choice(s_ids),'employee_id':random.choice(e_ids),
             'order_status':random.choice(['Completed','Shipped','Pending','Cancelled'])} for i in range(30000)]
    pd.DataFrame(orders).to_csv(os.path.join(FLAT_FILE_DIR,'Sales_Export.csv'),index=False)

    # Marketing API mock
    os.makedirs(API_MOCK_DIR, exist_ok=True)
    campaigns=[{'campaign_id':f"CAMP-{500+i}",'campaign_name':f"{fake.color_name()} {fake.job()} Promo",
                'campaign_type':random.choice(['Growth','Retention','Branding']),
                'channel':random.choice(['Email','Social Media','Search','Affiliate']),
                'budget_usd':round(random.uniform(1000,100000),2),
                'status':random.choice(['Completed','Active','Planned']),
                'start_date':fake.date_between(start_date='-2y',end_date='-1y').strftime('%Y-%m-%d'),
                'end_date':fake.date_between(start_date='-1y',end_date='today').strftime('%Y-%m-%d')} for i in range(50)]
    with open(os.path.join(API_MOCK_DIR,'marketing_campaigns.json'),'w') as f:
        json.dump({'data':campaigns},f,indent=4)
    print("Retail Source Generation Complete.")
    return c_ids, p_ids, s_ids, e_ids


# ============================================================
# FINANCE GENERATORS (stub — extend as needed)
# ============================================================
def generate_finance():
    print("Generating Finance Source (GL/AR/AP)...")
    # Add your finance source generators here following the same pattern
    # as healthcare and retail above.
    print("Finance Source Generation Complete.")


# ============================================================
# ENTRY POINT — reads config.yaml, dispatches to correct generator
# ============================================================
if __name__ == "__main__":
    domain = get_active_domain()
    print(f"--- Starting Source Data Generation for domain: {domain.upper()} ---")
    if domain == 'healthcare':
        generate_healthcare()
    elif domain == 'retail':
        generate_retail()
    elif domain == 'finance':
        generate_finance()
    else:
        print(f"[WARNING] No generator defined for domain '{domain}'. Add one to generate_sources.py.")
    print(f"--- Source Data Generation Complete ({domain}) ---")
