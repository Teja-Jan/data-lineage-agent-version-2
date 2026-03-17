import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')

def setup_data_warehouse():
    print(f"Setting up Target Data Warehouse at {TARGET_DB_PATH}...")
    
    os.makedirs(os.path.dirname(TARGET_DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()

    # Drop existing tables just in case
    tables = [
        'Dim_Customer', 'Dim_Product', 'Dim_Date', 'Dim_Campaign', 
        'Dim_Store', 'Dim_Employee', 'Dim_Promotion', 'Dim_Supplier', 'Dim_Geography',
        'Fact_Sales', 'Fact_Inventory', 'etl_execution_logs', 'db_audit_log'
    ]
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
    # Dimension: Customer (Expanded to ~15 columns)
    cursor.execute('''
        CREATE TABLE Dim_Customer (
            customer_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            source_customer_id INTEGER,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone_number TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            registration_date DATE,
            loyalty_score INTEGER,
            customer_segment TEXT,
            system_status TEXT,
            is_current BOOLEAN,
            effective_start_date DATE,
            effective_end_date DATE
        )
    ''')
    
    # Dimension: Product (Expanded to ~12 columns)
    cursor.execute('''
        CREATE TABLE Dim_Product (
            product_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            source_product_id INTEGER,
            product_name TEXT,
            category TEXT,
            sub_category TEXT,
            price REAL,
            cost_price REAL,
            supplier_id INTEGER,
            warehouse_location TEXT,
            is_active BOOLEAN,
            launch_date DATE
        )
    ''')

    # Dimension: Date
    cursor.execute('''
        CREATE TABLE Dim_Date (
            date_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            quarter INTEGER,
            day_of_week TEXT,
            is_weekend BOOLEAN
        )
    ''')

    # Dimension: Campaign
    cursor.execute('''
        CREATE TABLE Dim_Campaign (
            campaign_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            source_campaign_id TEXT,
            campaign_name TEXT,
            campaign_type TEXT,
            channel TEXT,
            budget REAL,
            status TEXT,
            owner TEXT
        )
    ''')

    # Fact: Sales (Expanded to ~12 columns)
    cursor.execute('''
        CREATE TABLE Fact_Sales (
            sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT,
            customer_sk INTEGER,
            product_sk INTEGER,
            date_sk INTEGER,
            campaign_sk INTEGER,
            store_sk INTEGER,
            employee_sk INTEGER,
            promotion_sk INTEGER,
            quantity INTEGER,
            unit_price REAL,
            discount_amount REAL,
            tax_amount REAL,
            total_amount REAL,
            margin_amount REAL,
            payment_method TEXT,
            shipping_method TEXT,
            region TEXT,
            FOREIGN KEY (customer_sk) REFERENCES Dim_Customer(customer_sk),
            FOREIGN KEY (product_sk) REFERENCES Dim_Product(product_sk),
            FOREIGN KEY (date_sk) REFERENCES Dim_Date(date_sk),
            FOREIGN KEY (campaign_sk) REFERENCES Dim_Campaign(campaign_sk)
        )
    ''')

    # Dimension: Store
    cursor.execute('''
        CREATE TABLE Dim_Store (
            store_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            store_number TEXT,
            store_name TEXT,
            store_type TEXT,
            floor_space_sqft INTEGER,
            open_date DATE,
            manager_name TEXT,
            geography_sk INTEGER
        )
    ''')

    # Dimension: Employee
    cursor.execute('''
        CREATE TABLE Dim_Employee (
            employee_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id TEXT,
            first_name TEXT,
            last_name TEXT,
            job_title TEXT,
            department TEXT,
            hire_date DATE,
            store_sk INTEGER
        )
    ''')

    # Dimension: Promotion
    cursor.execute('''
        CREATE TABLE Dim_Promotion (
            promotion_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            promo_code TEXT,
            promo_name TEXT,
            discount_pct REAL,
            start_date DATE,
            end_date DATE,
            promo_type TEXT
        )
    ''')

    # Dimension: Supplier
    cursor.execute('''
        CREATE TABLE Dim_Supplier (
            supplier_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name TEXT,
            contact_name TEXT,
            contact_email TEXT,
            country TEXT,
            rating INTEGER
        )
    ''')
    
    # Dimension: Geography
    cursor.execute('''
        CREATE TABLE Dim_Geography (
            geography_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            state TEXT,
            country TEXT,
            postal_code TEXT,
            region TEXT
        )
    ''')

    # Fact: Inventory
    cursor.execute('''
        CREATE TABLE Fact_Inventory (
            inventory_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_sk INTEGER,
            product_sk INTEGER,
            store_sk INTEGER,
            supplier_sk INTEGER,
            quantity_on_hand INTEGER,
            quantity_on_order INTEGER,
            reorder_level INTEGER,
            unit_cost REAL,
            total_inventory_value REAL,
            FOREIGN KEY (date_sk) REFERENCES Dim_Date(date_sk),
            FOREIGN KEY (product_sk) REFERENCES Dim_Product(product_sk),
            FOREIGN KEY (store_sk) REFERENCES Dim_Store(store_sk)
        )
    ''')

    # ETL Logs (Enhanced Telemetry)
    cursor.execute('''
        CREATE TABLE etl_execution_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT,
            source_system TEXT,
            target_table TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            records_read INTEGER,
            records_inserted INTEGER,
            records_updated INTEGER,
            transformation_metrics TEXT, -- JSON blob for stats
            status TEXT,
            error_message TEXT,
            retry_attempts INTEGER DEFAULT 0
        )
    ''')
    
    # DB Audit Logs for Lineage / Impact tracking
    cursor.execute('''
        CREATE TABLE db_audit_log (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,  -- DML, DDL, DCL
            target_object TEXT, -- Table Name, Pipeline Name
            changed_by_user TEXT,
            change_description TEXT,
            impact_score INTEGER DEFAULT 0
        )
    ''')

    conn.commit()
    conn.close()
    print("Target Data Warehouse Schema Initialization Complete.")

if __name__ == "__main__":
    setup_data_warehouse()
