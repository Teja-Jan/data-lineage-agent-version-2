import sqlite3
import datetime

conn = sqlite3.connect('data/target_dw/target_system.db')
cursor = conn.cursor()

# Get two recent execution IDs to corrupt
cursor.execute("SELECT log_id, start_time FROM etl_execution_logs WHERE pipeline_name = 'OLTP_TO_DW_CUSTOMER' ORDER BY log_id DESC LIMIT 1 OFFSET 3")
row1 = cursor.fetchone()
cursor.execute("SELECT log_id, start_time FROM etl_execution_logs WHERE pipeline_name = 'FLATFILE_TO_DW_INVENTORY' ORDER BY log_id DESC LIMIT 1 OFFSET 2")
row2 = cursor.fetchone()

if row1:
    cursor.execute("""
        UPDATE etl_execution_logs 
        SET status = 'FAILED', 
            error_message = 'Connection Timeout: Network error connecting to DB instance [Code: ERR_502]', 
            retry_attempts = 2 
        WHERE log_id = ?
    """, (row1[0],))
    print(f"Injected failure into OLTP_TO_DW_CUSTOMER on {row1[1]}")

if row2:
    cursor.execute("""
        UPDATE etl_execution_logs 
        SET status = 'FAILED', 
            error_message = 'Data Type Mismatch: Cannot cast OOS to INT in Fact_Inventory.quantity_on_hand', 
            retry_attempts = 1 
        WHERE log_id = ?
    """, (row2[0],))
    print(f"Injected failure into FLATFILE_TO_DW_INVENTORY on {row2[1]}")

conn.commit()
conn.close()
