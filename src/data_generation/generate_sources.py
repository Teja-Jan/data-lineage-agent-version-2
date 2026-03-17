import sqlite3
import pandas as pd
import json
import os
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, 'data', 'source_rdbms', 'source_system.db')
FLAT_FILE_DIR = os.path.join(BASE_DIR, 'data', 'source_flatfiles')
API_MOCK_DIR = os.path.join(BASE_DIR, 'data', 'source_api')

NUM_CUSTOMERS = 3000
NUM_PRODUCTS = 600
NUM_ORDERS = 50000

def create_rdbms_source():
    """Generates a SQLite Database representing an OLTP system with 15+ columns"""
    print(f"Generating RDBMS Source at {DB_PATH}...")
    
    # Ensure dir exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS Customers')
    cursor.execute('DROP TABLE IF EXISTS Products')
    cursor.execute('DROP TABLE IF EXISTS Stores')
    cursor.execute('DROP TABLE IF EXISTS Employees')
    cursor.execute('DROP TABLE IF EXISTS Suppliers')
    
    # Create Customers Table with ~18 columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone_number TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            zip_code TEXT,
            registration_date DATE,
            last_login_timestamp TIMESTAMP,
            loyalty_score INTEGER,
            customer_segment TEXT,
            is_newsletter_subscriber BOOLEAN,
            preferred_language TEXT,
            system_status TEXT,
            notes TEXT
        )
    ''')

    # Create Products Table with ~14 columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            sub_category TEXT,
            price REAL,
            cost_price REAL,
            supplier_id INTEGER,
            stock_quantity INTEGER,
            warehouse_location TEXT,
            weight_kg REAL,
            dimensions TEXT,
            is_active BOOLEAN,
            launch_date DATE,
            description TEXT
        )
    ''')

    # Create Store Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Stores (
            store_id INTEGER PRIMARY KEY,
            store_name TEXT,
            store_type TEXT,
            floor_space_sqft INTEGER,
            open_date DATE,
            manager_name TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            postal_code TEXT,
            region TEXT
        )
    ''')

    # Create Employee Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Employees (
            employee_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            job_title TEXT,
            department TEXT,
            hire_date DATE,
            store_id INTEGER
        )
    ''')

    # Create Suppliers Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Suppliers (
            supplier_id INTEGER PRIMARY KEY,
            supplier_name TEXT,
            contact_name TEXT,
            contact_email TEXT,
            country TEXT,
            rating INTEGER
        )
    ''')

    # Insert Synthetic Data: Customers
    customers_data = []
    for i in range(NUM_CUSTOMERS):
        customers_data.append((
            10000 + i,
            fake.first_name(),
            fake.last_name(),
            fake.ascii_email(),
            fake.phone_number(),
            fake.street_address().replace('\n', ', '),
            fake.city(),
            fake.state_abbr(),
            "USA",
            fake.zipcode(),
            fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
            fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            random.randint(0, 1000),
            random.choice(['Gold', 'Silver', 'Bronze', 'Platinum']),
            random.choice([0, 1]),
            random.choice(['en', 'es', 'fr', 'de']),
            random.choice(['Active', 'Inactive', 'Suspended']),
            fake.sentence() if random.random() > 0.5 else None
        ))
    
    cursor.executemany('INSERT INTO Customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', customers_data)

    # Insert Synthetic Data: Products
    products_data = []
    for i in range(NUM_PRODUCTS):
        products_data.append((
            20000 + i,
            fake.catch_phrase(),
            random.choice(['Electronics', 'Clothing', 'Home', 'Toys', 'Books']),
            random.choice(['Mobile', 'Kitchen', 'Apparel', 'Educational', 'Fiction']),
            round(random.uniform(10.0, 500.0), 2),
            round(random.uniform(5.0, 250.0), 2),
            fake.random_int(min=1, max=50),
            random.randint(0, 1000),
            f"WH-{random.choice(['A', 'B', 'C'])}-{random.randint(1, 100)}",
            round(random.uniform(0.1, 20.0), 2),
            f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)}",
            random.choice([0, 1]),
            fake.date_between(start_date='-10y', end_date='-1y').strftime('%Y-%m-%d'),
            fake.text(max_nb_chars=100)
        ))
        
    cursor.executemany('INSERT INTO Products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', products_data)

    conn.commit()
    
    # Insert Synthetic Data: Stores
    stores_data = []
    for i in range(50):
        stores_data.append((
            100 + i,
            f"{fake.city()} Superstore",
            random.choice(['Retail', 'Outlet', 'Flagship']),
            random.randint(5000, 50000),
            fake.date_between(start_date='-20y', end_date='-1y').strftime('%Y-%m-%d'),
            fake.name(),
            fake.city(),
            fake.state_abbr(),
            "USA",
            fake.zipcode(),
            random.choice(['North', 'South', 'East', 'West', 'Central'])
        ))
    cursor.executemany('INSERT INTO Stores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', stores_data)
    
    # Insert Synthetic Data: Employees
    employees_data = []
    store_ids = [s[0] for s in stores_data]
    for i in range(500):
        employees_data.append((
            3000 + i,
            fake.first_name(),
            fake.last_name(),
            random.choice(['Cashier', 'Manager', 'Stock Clerk', 'Customer Service']),
            random.choice(['Sales', 'Operations', 'Management']),
            fake.date_between(start_date='-10y', end_date='now').strftime('%Y-%m-%d'),
            random.choice(store_ids)
        ))
    cursor.executemany('INSERT INTO Employees VALUES (?, ?, ?, ?, ?, ?, ?)', employees_data)
    
    # Insert Synthetic Data: Suppliers
    suppliers_data = []
    for i in range(50):
        suppliers_data.append((
            1 + i,
            fake.company(),
            fake.name(),
            fake.company_email(),
            fake.country(),
            random.randint(1, 100)
        ))
    cursor.executemany('INSERT INTO Suppliers VALUES (?, ?, ?, ?, ?, ?)', suppliers_data)

    conn.commit()
    conn.close()
    
    return [c[0] for c in customers_data], [p[0] for p in products_data], store_ids, [e[0] for e in employees_data], [s[0] for s in suppliers_data]

def create_flatfile_source(customer_ids, product_ids, store_ids, employee_ids):
    """Generates a CSV flat file with 15+ columns representing sales data"""
    print("Generating Flat File Source (Sales_Export.csv)...")
    
    os.makedirs(FLAT_FILE_DIR, exist_ok=True)
    
    orders = []
    start_date = datetime.now() - timedelta(days=730) # 2 years of data
    
    for i in range(NUM_ORDERS):
        order_date = start_date + timedelta(days=random.randint(0, 730))
        orders.append({
            'transaction_id': f"TXN-{100000 + i}",
            'customer_id': random.choice(customer_ids),      
            'product_id': random.choice(product_ids),      
            'quantity': random.randint(1, 15),
            'discount_percent': round(random.uniform(0.0, 0.3), 2),
            'tax_rate': 0.08,
            'order_timestamp': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Apple Pay', 'Google Pay']),
            'shipping_method': random.choice(['Standard', 'Express', 'Overnight', 'Store Pickup']),
            'store_id': random.choice(store_ids),
            'employee_id': random.choice(employee_ids),
            'region': random.choice(['North', 'South', 'East', 'West', 'Central']),
            'currency': 'USD',
            'order_status': random.choice(['Completed', 'Shipped', 'Pending', 'Cancelled']),
            'is_gift': random.choice([0, 1]),
            'coupon_code': fake.bothify(text='PROMO##') if random.random() > 0.8 else None,
            'ip_address': fake.ipv4()
        })
        
    df = pd.DataFrame(orders)
    df.to_csv(os.path.join(FLAT_FILE_DIR, 'Sales_Export.csv'), index=False)
    
    # Inventory Snapshot CSV
    inventory = []
    for _ in range(10000):
        inventory_date = start_date + timedelta(days=random.randint(0, 730))
        inventory.append({
            'snapshot_date': inventory_date.strftime('%Y-%m-%d'),
            'product_id': random.choice(product_ids),
            'store_id': random.choice(store_ids),
            'supplier_id': random.randint(1, 50),
            'qty_on_hand': random.randint(0, 500),
            'qty_on_order': random.randint(0, 100),
            'reorder_level': random.randint(20, 100),
            'unit_cost': round(random.uniform(5.0, 250.0), 2)
        })
    df_inv = pd.DataFrame(inventory)
    df_inv.to_csv(os.path.join(FLAT_FILE_DIR, 'Inventory_Snapshot.csv'), index=False)


def create_api_source():
    """Generates JSON files simulating Marketing API data with more fields"""
    print("Generating API Source Mocks (marketing_campaigns.json)...")
    
    os.makedirs(API_MOCK_DIR, exist_ok=True)
    
    campaigns = []
    for i in range(50):
        campaigns.append({
            "campaign_id": f"CAMP-{500 + i}",
            "campaign_name": f"{fake.color_name().capitalize()} {fake.job()} Promo",
            "campaign_type": random.choice(["Growth", "Retention", "Branding", "Direct Mail"]),
            "channel": random.choice(["Email", "Social Media", "Search", "Affiliate", "SMS"]),
            "budget_usd": round(random.uniform(1000, 100000), 2),
            "actual_spend": round(random.uniform(500, 90000), 2),
            "start_date": fake.date_between(start_date='-2y', end_date='-1y').strftime('%Y-%m-%d'),
            "end_date": fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            "target_audience": random.choice(["Youth", "Professionals", "Seniors", "Parents"]),
            "status": random.choice(["Completed", "Active", "Planned", "Suspended"]),
            "performance_metrics": {
                "clicks": random.randint(100, 50000),
                "impressions": random.randint(5000, 1000000),
                "conversions": random.randint(10, 2000),
                "ctr": round(random.uniform(0.1, 5.0), 2),
                "roi": round(random.uniform(0.5, 4.0), 2)
            },
            "owner": fake.name()
        })
        
    with open(os.path.join(API_MOCK_DIR, 'marketing_campaigns.json'), 'w') as f:
        json.dump(campaigns, f, indent=4)


if __name__ == "__main__":
    print("--- Starting Source Data Generation ---")
    customer_ids, product_ids, store_ids, employee_ids, supplier_ids = create_rdbms_source()
    create_flatfile_source(customer_ids, product_ids, store_ids, employee_ids)
    create_api_source()
    print("--- Source Data Generation Complete ---")
