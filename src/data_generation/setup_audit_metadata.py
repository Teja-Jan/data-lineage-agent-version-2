import sqlite3
import os
from datetime import datetime, timedelta
import random

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')

def setup_audit_and_metadata():
    print(f"Setting up Audit & Metadata tracking at {TARGET_DB_PATH}...")
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()

    # Create ETL Pipeline Audit table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS etl_pipeline_audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT,
            modification_time TIMESTAMP,
            modified_by TEXT,
            version_tag TEXT,
            change_summary TEXT
        )
    ''')

    # Create Data Lineage Map table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_lineage_map (
            map_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system TEXT,
            source_table TEXT,
            source_column TEXT,
            target_table TEXT,
            target_column TEXT,
            transformation_rule TEXT
        )
    ''')
    
    # Clear existing data to ensure idempotency
    cursor.execute('DELETE FROM etl_pipeline_audit')
    cursor.execute('DELETE FROM data_lineage_map')
    cursor.execute('DELETE FROM db_audit_log')
    cursor.execute('DELETE FROM report_dependency')
    cursor.execute('DELETE FROM table_catalog')

    # Seed Lineage Map (Column Level covering new schema)
    mappings = [
        # Customers -> Dim_Customer
        ('SQLite:OLTP', 'Customers', 'customer_id', 'Dim_Customer', 'source_customer_id', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'first_name', 'Dim_Customer', 'first_name', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'last_name', 'Dim_Customer', 'last_name', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'email', 'Dim_Customer', 'email', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'phone_number', 'Dim_Customer', 'phone_number', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'address', 'Dim_Customer', 'address', 'Formatting: Concatenation'),
        ('SQLite:OLTP', 'Customers', 'city', 'Dim_Customer', 'city', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'state', 'Dim_Customer', 'state', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'zip_code', 'Dim_Customer', 'zip_code', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'registration_date', 'Dim_Customer', 'registration_date', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'loyalty_score', 'Dim_Customer', 'loyalty_score', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'customer_segment', 'Dim_Customer', 'customer_segment', 'Direct Mapping'),
        ('SQLite:OLTP', 'Customers', 'system_status', 'Dim_Customer', 'system_status', 'Direct Mapping'),
        
        # Products -> Dim_Product
        ('SQLite:OLTP', 'Products', 'product_id', 'Dim_Product', 'source_product_id', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'product_name', 'Dim_Product', 'product_name', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'category', 'Dim_Product', 'category', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'sub_category', 'Dim_Product', 'sub_category', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'price', 'Dim_Product', 'price', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'cost_price', 'Dim_Product', 'cost_price', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'supplier_id', 'Dim_Product', 'supplier_id', 'Direct Mapping'),
        ('SQLite:OLTP', 'Products', 'warehouse_location', 'Dim_Product', 'warehouse_location', 'Direct Mapping'),
        
        # Sales -> Fact_Sales
        ('CSV:Sales', 'Sales_Export', 'transaction_id', 'Fact_Sales', 'transaction_id', 'Direct Mapping'),
        ('CSV:Sales', 'Sales_Export', 'quantity', 'Fact_Sales', 'quantity', 'Direct Mapping'),
        ('CSV:Sales', 'Sales_Export', 'order_timestamp', 'Fact_Sales', 'date_sk', 'Lookup: Dim_Date'),
        ('CSV:Sales', 'Sales_Export', 'payment_method', 'Fact_Sales', 'payment_method', 'Direct Mapping'),
        ('CSV:Sales', 'Sales_Export', 'shipping_method', 'Fact_Sales', 'shipping_method', 'Direct Mapping'),
        ('CSV:Sales', 'Sales_Export', 'region', 'Fact_Sales', 'region', 'Direct Mapping'),
        ('CSV:Sales', 'Sales_Export', 'discount_percent', 'Fact_Sales', 'discount_amount', 'Calculation: price * qty * disc%'),
        ('CSV:Sales', 'Sales_Export', 'tax_rate', 'Fact_Sales', 'tax_amount', 'Calculation: base * tax%'),
        
        # Marketing -> Dim_Campaign
        ('JSON:API', 'marketing_campaigns', 'campaign_id', 'Dim_Campaign', 'source_campaign_id', 'JSON Extract'),
        ('JSON:API', 'marketing_campaigns', 'campaign_name', 'Dim_Campaign', 'campaign_name', 'JSON Extract'),
        ('JSON:API', 'marketing_campaigns', 'budget_usd', 'Dim_Campaign', 'budget', 'JSON Extract'),
        ('JSON:API', 'marketing_campaigns', 'channel', 'Dim_Campaign', 'channel', 'JSON Extract'),
        
        # New Tables Mapping
        ('SQLite:OLTP', 'Stores', 'store_id', 'Dim_Store', 'store_number', 'Direct Mapping'),
        ('SQLite:OLTP', 'Stores', 'store_name', 'Dim_Store', 'store_name', 'Direct Mapping'),
        ('SQLite:OLTP', 'Stores', 'floor_space', 'Dim_Store', 'floor_space_sqft', 'Direct Mapping'),
        
        ('SQLite:OLTP', 'Employees', 'emp_id', 'Dim_Employee', 'employee_id', 'Direct Mapping'),
        ('SQLite:OLTP', 'Employees', 'first_name', 'Dim_Employee', 'first_name', 'Direct Mapping'),
        ('SQLite:OLTP', 'Employees', 'job_title', 'Dim_Employee', 'job_title', 'Direct Mapping'),
        
        ('SQLite:OLTP', 'Promotions', 'promo_code', 'Dim_Promotion', 'promo_code', 'Direct Mapping'),
        ('SQLite:OLTP', 'Promotions', 'discount_pct', 'Dim_Promotion', 'discount_pct', 'Direct Mapping'),
        
        ('SQLite:OLTP', 'Suppliers', 'supplier_name', 'Dim_Supplier', 'supplier_name', 'Direct Mapping'),
        ('SQLite:OLTP', 'Suppliers', 'rating', 'Dim_Supplier', 'rating', 'Direct Mapping'),
        
        ('SQLite:OLTP', 'Geography', 'city', 'Dim_Geography', 'city', 'Direct Mapping'),
        ('SQLite:OLTP', 'Geography', 'region', 'Dim_Geography', 'region', 'Direct Mapping'),
        
        ('CSV:Inventory', 'Inventory_Snapshot', 'quantity_on_hand', 'Fact_Inventory', 'quantity_on_hand', 'Direct Mapping'),
        ('CSV:Inventory', 'Inventory_Snapshot', 'unit_cost', 'Fact_Inventory', 'unit_cost', 'Direct Mapping'),
        ('CSV:Inventory', 'Inventory_Snapshot', 'product_id', 'Fact_Inventory', 'product_sk', 'Lookup: Dim_Product'),
        
        ('ERP:Fiscal_Calendar', 'Fiscal_Calendar_Export', 'full_date', 'Dim_Date', 'full_date', 'ERP Extract'),
        ('ERP:Fiscal_Calendar', 'Fiscal_Calendar_Export', 'year', 'Dim_Date', 'year', 'ERP Extract')
    ]
    cursor.executemany('''
        INSERT INTO data_lineage_map (source_system, source_table, source_column, target_table, target_column, transformation_rule)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', mappings)

    # Seed DB Audit Logs (Historical events last 2 years)
    event_templates = [
        ('DDL', '{table}', '{user}', 'Modified schema: added {col} column', random.randint(60, 95)),
        ('DDL', '{table}', '{user}', 'Modified datatype of {col} from FLOAT to DECIMAL', random.randint(75, 85)),
        ('DDL', '{table}', '{user}', 'Created index on {col} for performance', random.randint(10, 30)),
        ('DCL', '{table}', 'sec_admin', 'Granted SELECT to {role}', random.randint(10, 40)),
        ('DCL', '{table}', 'sec_admin', 'Revoked UPDATE from {role}', random.randint(70, 90)),
        ('DML', '{table}', '{user}', 'Bulk update of {col} for data cleanup', random.randint(40, 70)),
        ('DML', 'etl_execution_logs', 'system', 'Archived historical logs', 10),
    ]
    
    tables = ['Dim_Customer', 'Dim_Product', 'Fact_Sales', 'Dim_Campaign', 'Dim_Date']
    users = ['admin_1', 'dev_user_a', 'data_architect', 'sys_admin', 'analyst_01']
    roles = ['Financial_Analyst', 'Marketing_Lead', 'Data_Scientist', 'Business_Unit_A']
    cols = ['loyalty_score', 'cost_price', 'discount_amount', 'margin_amt', 'region', 'phone_number', 'price']

    audit_inserts = []
    base_time = datetime.now() - timedelta(days=730)
    for i in range(180): # Increased count
        tmpl = random.choice(event_templates)
        table = random.choice(tables)
        user = random.choice(users)
        role = random.choice(roles)
        col = random.choice(cols)
        
        base_time += timedelta(hours=random.randint(12, 100))
        if base_time > datetime.now(): break
        
        desc = tmpl[3].format(table=table, user=user, role=role, col=col)
        audit_inserts.append((
            base_time.strftime('%Y-%m-%d %H:%M:%S'),
            tmpl[0], table, user, desc, tmpl[4]
        ))
        
    cursor.executemany('''
        INSERT INTO db_audit_log (event_time, event_type, target_object, changed_by_user, change_description, impact_score)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', audit_inserts)

    # Seed ETL Pipeline Audit Logs (More granular versioning)
    pipeline_audits = [
        ('OLTP_TO_DW_CUSTOMER', 'admin_1', 'v1.0.0', 'Initial baseline for Dim_Customer load'),
        ('OLTP_TO_DW_CUSTOMER', 'dev_user_a', 'v2.1.0', 'Integrated loyalty_score mapping from Source RDBMS'),
        ('OLTP_TO_DW_CUSTOMER', 'sys_admin', 'v2.2.0', 'Optimized SCD Type 1 logic for faster merges'),
        ('FLATFILE_TO_DW_SALES', 'admin_1', 'v1.0.0', 'Initial Sales ingestion from CSV'),
        ('FLATFILE_TO_DW_SALES', 'data_eng_blue', 'v2.0.0', 'Major refactor for margin and tax calculations'),
        ('FLATFILE_TO_DW_SALES', 'sys_admin', 'v2.1.0', 'Added Tax ID validation and NULL handling'),
        ('API_TO_DW_CAMPAIGN', 'dev_user_a', 'v1.0.0', 'Initial JSON campaign ingestion'),
        ('API_TO_DW_CAMPAIGN', 'data_eng_red', 'v1.5.2', 'Added performance metrics extraction'),
        ('OLTP_TO_DW_PRODUCT', 'data_eng_green', 'v1.1.0', 'Added cost_price extraction for margin analysis'),
    ]
    
    pl_audit_inserts = []
    base_time = datetime.now() - timedelta(days=600)
    for pl in pipeline_audits:
        base_time += timedelta(days=random.randint(40, 60))
        pl_audit_inserts.append((
            pl[0], base_time.strftime('%Y-%m-%d %H:%M:%S'), pl[1], pl[2], pl[3]
        ))
        
    cursor.executemany('''
        INSERT INTO etl_pipeline_audit (pipeline_name, modification_time, modified_by, version_tag, change_summary)
        VALUES (?, ?, ?, ?, ?)
    ''', pl_audit_inserts)

    # ---- Table Catalog (Data Model Documentation) ----
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS table_catalog (
            catalog_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT UNIQUE,
            table_type TEXT,
            source_system TEXT,
            etl_pipeline TEXT,
            business_description TEXT,
            update_strategy TEXT,
            linked_tables TEXT,
            key_measures TEXT
        )
    ''')
    cursor.execute("DELETE FROM table_catalog")
    table_docs = [
        ('Fact_Sales', 'FACT', 'CSV:Sales_Export + SQLite:OLTP', 'FLATFILE_TO_DW_SALES',
         'Captures each individual sales transaction. The grain is one row per order line item. '
         'It connects customers, products, dates, and campaigns to quantify business performance.',
         'APPEND (new records daily)',
         'Dim_Customer, Dim_Product, Dim_Date, Dim_Campaign',
         'quantity|Source: Sales_Export.quantity|Number of units sold in a single transaction line.'
         '||unit_price|Source: Dim_Product.price|The listed selling price of the product at time of sale.'
         '||gross_revenue|unit_price × quantity|Total revenue before any deductions. Reflects the full value of the sale.'
         '||discount_amount|price × quantity × discount_percent / 100|Total monetary discount applied to the order. Higher discounts reduce revenue.'
         '||tax_amount|gross_revenue × tax_rate / 100|Government-mandated tax collected on the sale. Passed on to authorities.'
         '||net_revenue|gross_revenue − discount_amount − tax_amount|Actual revenue received by the business after deductions.'
         '||margin_amt|gross_revenue − (cost_price × quantity)|Profit generated from the sale. Measures how much the business earns above product cost.'),

        ('Dim_Customer', 'DIMENSION', 'SQLite:OLTP (Customers table)', 'OLTP_TO_DW_CUSTOMER',
         'Conformed dimension describing the customer entity. Contains all demographic, behavioural, '
         'and loyalty attributes needed for customer segmentation and analytics.',
         'SCD Type 1 (overwrite on change)',
         'Fact_Sales (via customer_sk)',
         'loyalty_score|Scale: 0–100|A numeric score assigned to each customer based on purchase frequency and spend. Higher = more loyal.'
         '||customer_segment|Bronze / Silver / Gold / Platinum|Marketing tier derived from loyalty_score. Platinum = highest value customers.'),

        ('Dim_Product', 'DIMENSION', 'SQLite:OLTP (Products table)', 'OLTP_TO_DW_PRODUCT',
         'Describes every sellable product. Includes pricing, cost, supplier information, and '
         'warehouse location for inventory analytics and margin reporting.',
         'SCD Type 1 (overwrite on change)',
         'Fact_Sales (via product_sk)',
         'price|Source: Products.price (REAL)|The retail selling price of the product in USD.'
         '||cost_price|Source: Products.cost_price (REAL)|The cost paid to the supplier to acquire the product.'
         '||margin_pct|(price − cost_price) / price × 100|Percentage of the selling price that is profit. A margin of 40% means 40 cents of every dollar is profit.'),

        ('Dim_Date', 'DIMENSION', 'System-generated (date series)', 'DATE_DIM_LOAD',
         'Standard calendar dimension generated from a date series. Supports time-based aggregation '
         'at day, week, month, quarter, and year granularity.',
         'Pre-populated (static until 2030)',
         'Fact_Sales (via date_sk)',
         'year|INTEGER|Calendar year of the transaction (e.g. 2024).'
         '||quarter|INTEGER (1–4)|The fiscal quarter: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec.'
         '||month|INTEGER (1–12)|Month number. Used to roll up daily data to monthly reports.'
         '||day_of_week|TEXT (e.g. Monday)|Day name. Used to identify weekday vs weekend patterns.'
         '||is_weekend|BOOLEAN (0 or 1)|Flag showing if the transaction occurred on a weekend. Useful for retail trend comparisons.'),

        ('Dim_Campaign', 'DIMENSION', 'JSON:API (marketing_campaigns)', 'API_TO_DW_CAMPAIGN',
         'Conformed dimension tracking all marketing campaigns. Enables attribution of sales to '
         'specific campaigns, channels, and spending budgets.',
         'SCD Type 1 (JSON batch load)',
         'Fact_Sales (via campaign_sk)',
         'budget|Source: marketing_campaigns.budget_usd (REAL)|Total USD amount allocated to the campaign.'
         '||channel|Source: marketing_campaigns.channel (TEXT)|The medium used: Email, Social Media, TV, etc.'
         '||roi_pct|attributed_revenue / budget × 100|Return on Investment %. A value of 150 means the campaign generated 1.5x its cost.'),
         
        ('Fact_Inventory', 'FACT', 'CSV:Inventory_Snapshot', 'FLATFILE_TO_DW_INVENTORY',
         'Daily snapshot of inventory levels across all stores and products. Used to monitor stock health.',
         'APPEND (daily snapshots)',
         'Dim_Product, Dim_Store, Dim_Date, Dim_Supplier',
         'quantity_on_hand|Source: Inventory_Snapshot.qty_on_hand|Units currently physically in the store.'
         '||total_inventory_value|quantity_on_hand × unit_cost|Total capital tied up in current inventory.'),
         
        ('Dim_Store', 'DIMENSION', 'SQLite:OLTP (Stores)', 'OLTP_TO_DW_STORE',
         'Physical retail locations where sales and inventory are tracked.',
         'SCD Type 1',
         'Fact_Sales, Fact_Inventory',
         'floor_space_sqft|Source: Stores.floor_space_sqft|Square footage of the retail location.'),
         
        ('Dim_Employee', 'DIMENSION', 'SQLite:OLTP (Employees)', 'OLTP_TO_DW_EMPLOYEE',
         'Staff members working at stores or corporate offices.',
         'SCD Type 1',
         'Fact_Sales',
         ''),
         
        ('Dim_Supplier', 'DIMENSION', 'SQLite:OLTP (Suppliers)', 'OLTP_TO_DW_SUPPLIER',
         'Vendors who supply products for inventory.',
         'SCD Type 1',
         'Fact_Inventory, Dim_Product',
         'rating|Source: Suppliers.rating|Supplier quality and reliability score (1-100).'),
         
        ('Dim_Promotion', 'DIMENSION', 'SQLite:OLTP', 'OLTP_TO_DW_PROMO',
         'Discount codes and promotional events applied at checkout.',
         'SCD Type 1',
         'Fact_Sales',
         'discount_pct|Source: Promotions.discount|Percentage amount taken off the transaction.'),
         
        ('Dim_Geography', 'DIMENSION', 'SQLite:OLTP', 'OLTP_TO_DW_GEO',
         'Geographic regions for territory mapping.',
         'SCD Type 1',
         'Dim_Store',
         '')
    ]
    cursor.executemany('''
        INSERT OR REPLACE INTO table_catalog
        (table_name, table_type, source_system, etl_pipeline, business_description, update_strategy, linked_tables, key_measures)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', table_docs)

    # ---- Report Dependency Table ----
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS report_dependency (
            dep_id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_name TEXT,
            dw_table TEXT,
            dw_column TEXT,
            business_owner TEXT
        )
    ''')
    cursor.execute("DELETE FROM report_dependency")
    report_deps = [
        ('Sales Performance Dashboard', 'Fact_Sales', 'net_revenue', 'Finance Team'),
        ('Sales Performance Dashboard', 'Fact_Sales', 'quantity', 'Finance Team'),
        ('Sales Performance Dashboard', 'Dim_Product', 'price', 'Finance Team'),
        ('Customer Segmentation Report', 'Dim_Customer', 'customer_segment', 'Marketing Team'),
        ('Customer Segmentation Report', 'Dim_Customer', 'loyalty_score', 'Marketing Team'),
        ('Margin Analysis Report', 'Dim_Product', 'cost_price', 'Finance Team'),
        ('Margin Analysis Report', 'Fact_Sales', 'margin_amt', 'Finance Team'),
        ('Campaign ROI Report', 'Dim_Campaign', 'budget', 'Marketing Team'),
        ('Campaign ROI Report', 'Fact_Sales', 'discount_amount', 'Marketing Team'),
        ('Monthly Revenue Trend', 'Fact_Sales', 'gross_revenue', 'Executive Team'),
        ('Monthly Revenue Trend', 'Dim_Date', 'month_name', 'Executive Team'),
        ('Product Performance Report', 'Dim_Product', 'category', 'Analytics Team'),
        ('Product Performance Report', 'Fact_Sales', 'quantity', 'Analytics Team'),
        ('Tax Reconciliation Report', 'Fact_Sales', 'tax_amount', 'Finance Team'),
        ('Store Performance Dashboard', 'Fact_Sales', 'quantity', 'Retail Ops'),
        ('Store Performance Dashboard', 'Dim_Store', 'store_type', 'Retail Ops'),
        ('Inventory Health Report', 'Fact_Inventory', 'quantity_on_hand', 'Supply Chain'),
        ('Inventory Health Report', 'Dim_Supplier', 'rating', 'Supply Chain'),
    ]
    cursor.executemany('''
        INSERT INTO report_dependency (report_name, dw_table, dw_column, business_owner)
        VALUES (?, ?, ?, ?)
    ''', report_deps)

    conn.commit()
    conn.close()
    print("Audit & Metadata Seeding Complete.")

if __name__ == "__main__":
    setup_audit_and_metadata()
