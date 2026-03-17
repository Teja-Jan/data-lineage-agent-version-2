import sqlite3
import pandas as pd
import json
import os
from datetime import datetime
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOURCE_DB_PATH = os.path.join(BASE_DIR, 'data', 'source_rdbms', 'source_system.db')
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')
CSV_PATH = os.path.join(BASE_DIR, 'data', 'source_flatfiles', 'Sales_Export.csv')
JSON_PATH = os.path.join(BASE_DIR, 'data', 'source_api', 'marketing_campaigns.json')
ETL_LOG_DIR = os.path.join(BASE_DIR, 'logs', 'etl')

os.makedirs(ETL_LOG_DIR, exist_ok=True)


def write_etl_log_file(pipeline_name, source, target, start, end, read, inserted, updated, status, metrics, error):
    """Writes a structured ETL log entry to a daily rotating log file."""
    log_date = start.strftime('%Y-%m-%d')
    log_file = os.path.join(ETL_LOG_DIR, f"etl_{log_date}.log")
    duration_sec = round((end - start).total_seconds(), 2)
    entry = (
        f"[{start.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"PIPELINE={pipeline_name} | SOURCE={source} | TARGET={target} | "
        f"STATUS={status} | RECORDS_READ={read} | INSERTED={inserted} | UPDATED={updated} | "
        f"DURATION={duration_sec}s | METRICS={json.dumps(metrics or {})} | ERROR={error or 'None'}\n"
    )
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(entry)

def log_etl_execution(cursor, pipeline_name, source, target, start, end, read, inserted, updated, status, metrics=None, error=""):
    metrics_json = json.dumps(metrics) if metrics else "{}"
    cursor.execute('''
        INSERT INTO etl_execution_logs 
        (pipeline_name, source_system, target_table, start_time, end_time, 
         records_read, records_inserted, records_updated, transformation_metrics, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (pipeline_name, source, target, start.strftime('%Y-%m-%d %H:%M:%S'), 
          end.strftime('%Y-%m-%d %H:%M:%S'), read, inserted, updated, metrics_json, status, error))
    # Also write to physical log file
    write_etl_log_file(pipeline_name, source, target, start, end, read, inserted, updated, status, metrics, error)

def load_dim_customer(src_conn, tgt_conn, run_date=None):
    start_time = datetime.now()
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()
    
    query = """
    SELECT customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code, 
           registration_date, loyalty_score, customer_segment, system_status 
    FROM Customers
    """
    if run_date:
        query += f" WHERE registration_date <= '{run_date.strftime('%Y-%m-%d')}'"
        
    src_cursor.execute(query)
    customers = src_cursor.fetchall()
    
    inserted = 0
    updated = 0
    for row in customers:
        tgt_cursor.execute("SELECT customer_sk FROM Dim_Customer WHERE source_customer_id = ?", (row[0],))
        exists = tgt_cursor.fetchone()
        if not exists:
            tgt_cursor.execute('''
                INSERT INTO Dim_Customer (
                    source_customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code,
                    registration_date, loyalty_score, customer_segment, system_status, is_current, effective_start_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ''', row + (run_date.strftime('%Y-%m-%d') if run_date else start_time.strftime('%Y-%m-%d'),))
            inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    metrics = {"batch_id": run_date.strftime('%Y%m%d') if run_date else "LATEST", "data_quality_score": 0.98}
    log_etl_execution(tgt_cursor, "OLTP_TO_DW_CUSTOMER", "SQLite:Customers", "Dim_Customer", start_time, end_time, len(customers), inserted, updated, "SUCCESS", metrics)


def load_dim_product(src_conn, tgt_conn, run_date=None):
    start_time = datetime.now()
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()
    
    src_cursor.execute("SELECT product_id, product_name, category, sub_category, price, cost_price, supplier_id, warehouse_location, is_active, launch_date FROM Products")
    products = src_cursor.fetchall()
    
    inserted = 0
    for row in products:
        tgt_cursor.execute("SELECT product_sk FROM Dim_Product WHERE source_product_id = ?", (row[0],))
        if not tgt_cursor.fetchone():
            tgt_cursor.execute('''
                INSERT INTO Dim_Product (source_product_id, product_name, category, sub_category, price, cost_price, supplier_id, warehouse_location, is_active, launch_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', row)
            inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    log_etl_execution(tgt_cursor, "OLTP_TO_DW_PRODUCT", "SQLite:Products", "Dim_Product", start_time, end_time, len(products), inserted, 0, "SUCCESS")


def load_dim_campaign(tgt_conn):
    start_time = datetime.now()
    tgt_cursor = tgt_conn.cursor()
    
    if not os.path.exists(JSON_PATH): return

    with open(JSON_PATH, 'r') as f:
        campaigns = json.load(f)
        
    inserted = 0
    for c in campaigns:
        tgt_cursor.execute("SELECT campaign_sk FROM Dim_Campaign WHERE source_campaign_id = ?", (c['campaign_id'],))
        if not tgt_cursor.fetchone():
            tgt_cursor.execute('''
                INSERT INTO Dim_Campaign (source_campaign_id, campaign_name, campaign_type, channel, budget, status, owner)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (c['campaign_id'], c['campaign_name'], c['campaign_type'], c['channel'], c['budget_usd'], c['status'], c['owner']))
            inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    log_etl_execution(tgt_cursor, "API_TO_DW_CAMPAIGN", "JSON_API:Marketing", "Dim_Campaign", start_time, end_time, len(campaigns), inserted, 0, "SUCCESS")


def populate_dim_date(tgt_conn, start_year=2020, end_year=2026):
    tgt_cursor = tgt_conn.cursor()
    tgt_cursor.execute("SELECT COUNT(*) FROM Dim_Date")
    if tgt_cursor.fetchone()[0] > 0: return

    start_date = pd.to_datetime(f'{start_year}-01-01')
    end_date = pd.to_datetime(f'{end_year}-12-31')
    dates = pd.date_range(start_date, end_date)
    
    for d in dates:
        tgt_cursor.execute('''
            INSERT INTO Dim_Date (full_date, year, month, day, quarter, day_of_week, is_weekend)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (d.strftime('%Y-%m-%d'), d.year, d.month, d.day, d.quarter, d.day_name(), 1 if d.dayofweek >= 5 else 0))
    tgt_conn.commit()


def load_dim_store(src_conn, tgt_conn):
    start_time = datetime.now()
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()
    
    src_cursor.execute("SELECT store_id, store_name, store_type, floor_space_sqft, open_date, manager_name, city, state, country, postal_code, region FROM Stores")
    stores = src_cursor.fetchall()
    
    inserted = 0
    for row in stores:
        tgt_cursor.execute("SELECT store_sk FROM Dim_Store WHERE store_number = ?", (str(row[0]),))
        if not tgt_cursor.fetchone():
            tgt_cursor.execute('''
                INSERT INTO Dim_Store (store_number, store_name, store_type, floor_space_sqft, open_date, manager_name)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (str(row[0]), row[1], row[2], row[3], row[4], row[5]))
            inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    log_etl_execution(tgt_cursor, "OLTP_TO_DW_STORE", "SQLite:Stores", "Dim_Store", start_time, end_time, len(stores), inserted, 0, "SUCCESS")


def load_dim_employee(src_conn, tgt_conn):
    start_time = datetime.now()
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()
    
    src_cursor.execute("SELECT employee_id, first_name, last_name, job_title, department, hire_date FROM Employees")
    employees = src_cursor.fetchall()
    
    inserted = 0
    for row in employees:
        tgt_cursor.execute("SELECT employee_sk FROM Dim_Employee WHERE employee_id = ?", (str(row[0]),))
        if not tgt_cursor.fetchone():
            tgt_cursor.execute('''
                INSERT INTO Dim_Employee (employee_id, first_name, last_name, job_title, department, hire_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (str(row[0]), row[1], row[2], row[3], row[4], row[5]))
            inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    log_etl_execution(tgt_cursor, "OLTP_TO_DW_EMPLOYEE", "SQLite:Employees", "Dim_Employee", start_time, end_time, len(employees), inserted, 0, "SUCCESS")


def load_dim_supplier(src_conn, tgt_conn):
    start_time = datetime.now()
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()
    
    src_cursor.execute("SELECT supplier_id, supplier_name, contact_name, contact_email, country, rating FROM Suppliers")
    suppliers = src_cursor.fetchall()
    
    inserted = 0
    for row in suppliers:
        tgt_cursor.execute("SELECT supplier_sk FROM Dim_Supplier WHERE supplier_name = ?", (row[1],))
        if not tgt_cursor.fetchone():
            tgt_cursor.execute('''
                INSERT INTO Dim_Supplier (supplier_name, contact_name, contact_email, country, rating)
                VALUES (?, ?, ?, ?, ?)
            ''', (row[1], row[2], row[3], row[4], row[5]))
            inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    log_etl_execution(tgt_cursor, "OLTP_TO_DW_SUPPLIER", "SQLite:Suppliers", "Dim_Supplier", start_time, end_time, len(suppliers), inserted, 0, "SUCCESS")


def load_fact_sales(tgt_conn, run_date=None):
    start_time = run_date if run_date else datetime.now()
    tgt_cursor = tgt_conn.cursor()
    
    if not os.path.exists(CSV_PATH): return
    df = pd.read_csv(CSV_PATH)
    
    if run_date:
        date_str = run_date.strftime('%Y-%m-%d')
        df = df[df['order_timestamp'].str.startswith(date_str)]
    
    if df.empty: return

    # SK Mapping
    tgt_cursor.execute("SELECT source_customer_id, customer_sk FROM Dim_Customer")
    cust_map = dict(tgt_cursor.fetchall())
    tgt_cursor.execute("SELECT source_product_id, product_sk FROM Dim_Product")
    prod_map = dict(tgt_cursor.fetchall())
    tgt_cursor.execute("SELECT source_campaign_id, campaign_sk FROM Dim_Campaign")
    camp_map = dict(tgt_cursor.fetchall())
    camp_keys = list(camp_map.values())
    tgt_cursor.execute("SELECT store_number, store_sk FROM Dim_Store")
    store_map = dict(tgt_cursor.fetchall())
    tgt_cursor.execute("SELECT employee_id, employee_sk FROM Dim_Employee")
    emp_map = dict(tgt_cursor.fetchall())
    import random
    tgt_cursor.execute("SELECT full_date, date_sk FROM Dim_Date")
    date_map = dict(tgt_cursor.fetchall())

    # Get product details for margin calculation
    tgt_cursor.execute("SELECT product_sk, price, cost_price FROM Dim_Product")
    prod_details = {row[0]: (row[1], row[2]) for row in tgt_cursor.fetchall()}

    inserted = 0
    for _, row in df.iterrows():
        tgt_cursor.execute("SELECT sales_id FROM Fact_Sales WHERE transaction_id = ?", (row['transaction_id'],))
        if not tgt_cursor.fetchone():
            c_sk = cust_map.get(int(row['customer_id']))
            p_sk = prod_map.get(int(row['product_id']))
            d_sk = date_map.get(str(row['order_timestamp']).split(' ')[0])
            s_sk = store_map.get(str(row.get('store_id', '101')))
            e_sk = emp_map.get(str(row.get('employee_id', '3000')))
            cmp_sk = random.choice(camp_keys) if camp_keys else None
            
            if not c_sk or not p_sk or not d_sk: continue

            q = int(row['quantity'])
            price, cost = prod_details.get(p_sk, (0.0, 0.0))
            disc_pct = float(row['discount_percent'])
            tax_rate = float(row['tax_rate'])
            
            disc_amt = (price * q) * disc_pct
            base_total = (price * q) - disc_amt
            tax_amt = base_total * tax_rate
            total = base_total + tax_amt
            margin = base_total - (cost * q)

            tgt_cursor.execute('''
                INSERT INTO Fact_Sales (
                    transaction_id, customer_sk, product_sk, date_sk, campaign_sk, store_sk, employee_sk,
                    quantity, unit_price, discount_amount, tax_amount, total_amount, margin_amount,
                    payment_method, shipping_method, region
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row['transaction_id'], c_sk, p_sk, d_sk, cmp_sk, s_sk, e_sk, q, price, disc_amt, tax_amt, total, margin, 
                  row['payment_method'], row['shipping_method'], row['region']))
            inserted += 1

    tgt_conn.commit()
    from datetime import timedelta
    end_time = start_time + timedelta(seconds=len(df)*0.01)
    metrics = {
        "total_revenue": int(df['quantity'].sum() * 100), 
        "avg_discount": float(df['discount_percent'].mean())
    }
    log_etl_execution(tgt_cursor, "FLATFILE_TO_DW_SALES", "CSV:Sales_Export", "Fact_Sales", start_time, end_time, len(df), inserted, 0, "SUCCESS", metrics)


def load_fact_inventory(tgt_conn, run_date=None):
    start_time = run_date if run_date else datetime.now()
    tgt_cursor = tgt_conn.cursor()
    INV_CSV = os.path.join(BASE_DIR, 'data', 'source_flatfiles', 'Inventory_Snapshot.csv')
    if not os.path.exists(INV_CSV): return
    
    df = pd.read_csv(INV_CSV)
    if run_date:
        date_str = run_date.strftime('%Y-%m-%d')
        df = df[df['snapshot_date'] == date_str]
    if df.empty: return

    tgt_cursor.execute("SELECT source_product_id, product_sk FROM Dim_Product")
    prod_map = dict(tgt_cursor.fetchall())
    tgt_cursor.execute("SELECT store_number, store_sk FROM Dim_Store")
    store_map = dict(tgt_cursor.fetchall())
    tgt_cursor.execute("SELECT supplier_name, supplier_sk FROM Dim_Supplier")
    supp_keys = [r[1] for r in tgt_cursor.fetchall()]
    tgt_cursor.execute("SELECT full_date, date_sk FROM Dim_Date")
    date_map = dict(tgt_cursor.fetchall())
    import random

    inserted = 0
    for _, row in df.iterrows():
        p_sk = prod_map.get(int(row['product_id']))
        s_sk = store_map.get(str(row['store_id']))
        d_sk = date_map.get(str(row['snapshot_date']))
        sup_sk = random.choice(supp_keys) if supp_keys else None
        
        if not p_sk or not s_sk or not d_sk: continue
        
        q_on_hand = int(row['qty_on_hand'])
        u_cost = float(row['unit_cost'])
        tv = q_on_hand * u_cost

        tgt_cursor.execute('''
            INSERT INTO Fact_Inventory (
                date_sk, product_sk, store_sk, supplier_sk, 
                quantity_on_hand, quantity_on_order, reorder_level, unit_cost, total_inventory_value
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (d_sk, p_sk, s_sk, sup_sk, q_on_hand, int(row['qty_on_order']), int(row['reorder_level']), u_cost, tv))
        inserted += 1

    tgt_conn.commit()
    end_time = datetime.now()
    log_etl_execution(tgt_cursor, "FLATFILE_TO_DW_INVENTORY", "CSV:Inventory_Snapshot", "Fact_Inventory", start_time, end_time, len(df), inserted, 0, "SUCCESS")


def run_full_pipeline(run_date=None):
    import random
    src_conn = sqlite3.connect(SOURCE_DB_PATH)
    tgt_conn = sqlite3.connect(TARGET_DB_PATH)
    tgt_cursor = tgt_conn.cursor()
    
    # Randomly simulate failures for EDW scale simulation
    is_failure = random.random() < 0.05 # 5% chance of pipeline failure
    
    if is_failure:
        start_time = run_date if run_date else datetime.now()
        time.sleep(0.1) # simulate some work
        end_time = start_time + dt.timedelta(seconds=2)
        failures = [
            ("Connection Timeout", "Network error connecting to DB instance"),
            ("Null Constraint Violation", "Null value found in non-nullable column 'customer_id'"),
            ("API Rate Limit Exceeded", "Exceeded 429 Too Many Requests on Marketing API"),
            ("Data Type Mismatch", "Cannot cast 'OOS' to INT in Fact_Inventory.quantity_on_hand")
        ]
        fail_msg, fail_desc = random.choice(failures)
        bad_pipeline = random.choice(["OLTP_TO_DW_CUSTOMER", "API_TO_DW_CAMPAIGN", "FLATFILE_TO_DW_INVENTORY", "FLATFILE_TO_DW_SALES"])
        log_etl_execution(tgt_cursor, bad_pipeline, "VARIOUS", "VARIOUS", start_time, end_time, 1500, 0, 0, "FAILED", error=fail_desc)
        tgt_conn.commit()
        print(f"  [!] SIMULATED PIPELINE FAILURE: {bad_pipeline} - {fail_msg}")
        # Skip the rest to simulate abort
    else:    
        load_dim_customer(src_conn, tgt_conn, run_date)
        load_dim_product(src_conn, tgt_conn, run_date)
        load_dim_store(src_conn, tgt_conn)
        load_dim_employee(src_conn, tgt_conn)
        load_dim_supplier(src_conn, tgt_conn)
        load_dim_campaign(tgt_conn)
        load_fact_sales(tgt_conn, run_date)
        load_fact_inventory(tgt_conn, run_date)
    
    src_conn.close()
    tgt_conn.close()

if __name__ == "__main__":
    print("--- Starting ETL Processing ---")
    tgt_conn = sqlite3.connect(TARGET_DB_PATH)
    populate_dim_date(tgt_conn)
    tgt_conn.close()
    
    # Simulate history: Run for the last 30 days daily
    import datetime as dt
    today = dt.datetime.now()
    for i in range(30, -1, -1):
        d = today - dt.timedelta(days=i)
        print(f"Executing ETL for date: {d.strftime('%Y-%m-%d')}...")
        run_full_pipeline(d)
        
    print("--- ETL Processing Complete ---")

