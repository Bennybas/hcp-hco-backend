from flask import Flask, jsonify
from pyathena import connect
import pandas as pd
from flasgger import Swagger
from flask_cors import CORS
from dotenv import load_dotenv
import os 

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
swagger = Swagger(app)

def get_athena_data(query):
    conn = connect(
        aws_access_key_id=os.getenv("ATHENA_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ATHENA_SECRET_KEY"),
        region_name=os.getenv("ATHENA_REGION"),
        s3_staging_dir=os.getenv("S3_STAGING_DIR"),
        schema_name=os.getenv("ATHENA_DATABASE")
    )
    df = pd.read_sql(query, conn)
    return df

from flask import request  # Import request to handle parameters

@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    """
    Fetch Data from AWS Athena based on hcp_name
    """
    hcp_name = request.args.get('hcp_name')  # Get hcp_name from query params
    
    query = f"""
    SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
           hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
           hcp_state, hco_state 
    FROM "product_landing"."zolg_master"
    WHERE hcp_name = '{hcp_name}'
    """ if hcp_name else """
    SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
           hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
           hcp_state, hco_state 
    FROM "product_landing"."zolg_master"
    """
    
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))


@app.route('/fetch-map-data', methods=['GET'])
def fetch_map_data():
    """
    Fetch Map Data from AWS Athena
    """
    query = """
    WITH hcp_data AS (
    SELECT hcp_state, COUNT(DISTINCT hcp_name) AS hcp_count 
    FROM "product_landing"."zolg_master" GROUP BY hcp_state
), 
hco_data AS (
    SELECT hco_state, COUNT(DISTINCT hco_mdm) AS hco_count
    FROM "product_landing"."zolg_master" GROUP BY hco_state
)
SELECT DISTINCT zm.patient_id, zm.hcp_name, zm.hcp_city, zm.hcp_state, 
       zm.hcp_zip, zm.hco_mdm, zm.hco_mdm_name, zm.rend_hco_lat, 
       zm.rend_hco_long, zm.ref_hco_lat, zm.ref_hco_long, 
       hcp_data.hcp_count, hco_data.hco_count
FROM "product_landing"."zolg_master" zm
LEFT JOIN hcp_data ON zm.hcp_state = hcp_data.hcp_state
LEFT JOIN hco_data ON zm.hco_state = hco_data.hco_state
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))

@app.route('/fetch-hcplandscape-quarterpat', methods=['GET'])
def fetch_hcplandscape_quarterpat():
    """
    Fetch Patient Count by Quarter
    """
    query = """
    SELECT QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
           COUNT(DISTINCT patient_id) AS patient_count
    FROM "product_landing"."zolg_pat_hcp_data"
    WHERE YEAR(DATE_PARSE(month, '%d-%m-%Y')) = 2024
    GROUP BY QUARTER(DATE_PARSE(month, '%d-%m-%Y'))
    ORDER BY quarter DESC;
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))

@app.route('/fetch-hcplandscape-brand', methods=['GET'])
def fetch_hcplandscape_brand():
    """
    Fetch Patient Count by Drug per Quarter
    """
    query = """
    SELECT QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
           COUNT(DISTINCT CASE WHEN drug_name = 'ZOLGENSMA' THEN patient_id END) AS ZOLGENSMA,
           COUNT(DISTINCT CASE WHEN drug_name = 'EVRYSDI' THEN patient_id END) AS EVRYSDI,
           COUNT(DISTINCT CASE WHEN drug_name = 'SPINRAZA' THEN patient_id END) AS SPINRAZA
    FROM "product_landing"."zolg_pat_hcp_data"
    WHERE YEAR(DATE_PARSE(month, '%d-%m-%Y')) = 2024
    GROUP BY QUARTER(DATE_PARSE(month, '%d-%m-%Y'))
    ORDER BY quarter DESC;
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))

@app.route('/fetch-hcplandscape-kpicard', methods=['GET'])
def fetch_hcplandscape_kpicard():
    """
    Fetch KPI Card Data
    """
    query = """
    select count(DISTINCT hcp_id) as hcp_id,

 (SELECT 
  AVG(patient_count) AS avg_patients_per_hcp
FROM (
  SELECT 
    hcp_id, 
    COUNT(DISTINCT patient_id) AS patient_count
  FROM "product_landing"."zolg_master"
  GROUP BY hcp_id))AS patient_per_hcp,
  (SELECT 
  AVG(patient_count) AS avg_patients_per_hco
FROM (
  SELECT 
    hco_mdm, 
    COUNT(DISTINCT patient_id) AS patient_count
  FROM "product_landing"."zolg_master"
  GROUP BY hco_mdm))AS avg_patients_per_hco,
  
  (SELECT 
    COUNT(DISTINCT patient_id) AS total_patient_count
FROM "product_landing"."zolg_master"
WHERE 
    DATE_PARSE(month, '%d-%m-%Y') >= DATE_ADD('month', -12, CURRENT_DATE)) as pt_12_mth,
    (select count(DISTINCT ref_npi) from "product_landing"."zolg_master" where ref_npi !='-') as ref_npi
    
    from  "product_landing"."zolg_master"
    
    
    
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))

@app.route('/fetch-hcplandscape-insights', methods=['GET'])
def fetch_hcplandscape_insights():
    """
    Fetch HCP Landscape Insights
    """
    query = """
    SELECT DISTINCT hcp_id, patient_id, age_group, final_spec, hcp_segment, 
                    hco_mdm_name, hcp_name 
    FROM "product_landing"."zolg_master"
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
