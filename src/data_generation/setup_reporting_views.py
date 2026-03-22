import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')

def setup_reporting_views():
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()
    print(f"Creating SQL Views for Clinical Reporting in {TARGET_DB_PATH}...")

    # Drop existing Retail views
    cursor.execute("DROP VIEW IF EXISTS vw_sales_performance")
    cursor.execute("DROP VIEW IF EXISTS vw_inventory_optimization")
    cursor.execute("DROP VIEW IF EXISTS vw_customer_churn_risk")
    cursor.execute("DROP VIEW IF EXISTS vw_campaign_roi")

    # Drop existing Healthcare views if re-running
    cursor.execute("DROP VIEW IF EXISTS vw_hospital_readmission_risk")
    cursor.execute("DROP VIEW IF EXISTS vw_financial_claims_outcome")
    cursor.execute("DROP VIEW IF EXISTS vw_provider_performance_scorecard")
    cursor.execute("DROP VIEW IF EXISTS vw_regional_outbreak_heatmap")

    cursor.execute('''
        CREATE VIEW vw_hospital_readmission_risk AS
        SELECT 
            p.patient_sk,
            p.first_name || ' ' || p.last_name AS patient_name,
            d.icd10_code,
            d.description AS primary_diagnosis,
            d.severity_level,
            COUNT(e.encounter_id) AS total_encounters,
            MAX(e.date_sk) AS last_admission_date
        FROM Dim_Patient p
        JOIN Fact_Clinical_Encounter e ON p.patient_sk = e.patient_sk
        JOIN Fact_Claims c ON p.patient_sk = c.patient_sk
        JOIN Dim_Diagnosis d ON c.diagnosis_sk = d.diagnosis_sk
        GROUP BY p.patient_sk, patient_name, d.icd10_code, primary_diagnosis, d.severity_level
    ''')

    cursor.execute('''
        CREATE VIEW vw_financial_claims_outcome AS
        SELECT 
            p.provider_sk,
            p.full_name AS provider_name,
            p.department,
            c.claim_type,
            c.status,
            SUM(c.billed_amount) AS total_billed,
            SUM(c.allowed_amount) AS total_allowed,
            SUM(c.patient_responsibility) AS total_patient_resp,
            c.payer_name
        FROM Fact_Claims c
        JOIN Dim_Provider p ON c.provider_sk = p.provider_sk
        GROUP BY p.provider_sk, provider_name, p.department, c.claim_type, c.status, c.payer_name
    ''')
    
    cursor.execute('''
        CREATE VIEW vw_provider_performance_scorecard AS
        SELECT 
            pr.provider_sk,
            pr.full_name,
            pr.specialty,
            COUNT(DISTINCT e.encounter_id) AS total_encounters,
            AVG(e.encounter_duration_mins) AS avg_duration_mins,
            SUM(c.billed_amount) AS total_revenue_generated
        FROM Dim_Provider pr
        JOIN Fact_Clinical_Encounter e ON pr.provider_sk = e.provider_sk
        LEFT JOIN Fact_Claims c ON (pr.provider_sk = c.provider_sk AND e.date_sk = c.date_sk)
        GROUP BY pr.provider_sk, pr.full_name, pr.specialty
    ''')

    cursor.execute('''
        CREATE VIEW vw_regional_outbreak_heatmap AS
        SELECT 
            f.facility_type,
            f.city,
            f.state,
            d.icd10_code,
            d.clinical_category,
            COUNT(e.encounter_id) AS outbreak_case_count
        FROM Fact_Clinical_Encounter e
        JOIN Dim_Facility f ON e.facility_sk = f.facility_sk
        JOIN Fact_Claims c ON e.patient_sk = c.patient_sk
        JOIN Dim_Diagnosis d ON c.diagnosis_sk = d.diagnosis_sk
        GROUP BY f.facility_type, f.city, f.state, d.icd10_code, d.clinical_category
    ''')

    conn.commit()
    conn.close()
    print("Clinical SQL Views Successfully Created.")

if __name__ == "__main__":
    setup_reporting_views()
