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
    hcp_name = request.args.get('hcp_name')  
    
    query = f"""
    SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
           hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
           hcp_state, hco_state ,ref_hco_npi_mdm
    FROM "product_landing"."zolg_master"
    WHERE hcp_name = '{hcp_name}'
    """ if hcp_name else """
    SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
           hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
           hcp_state, hco_state ,ref_hco_npi_mdm
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
  
with uni as (
select distinct hcp_id,hcp_state,hcp_zip,hco_mdm,hco_state,hco_postal_cd_prim,patient_id from zolg_master_v2
union all 
select distinct ref_npi,ref_hcp_state,ref_hcp_zip,ref_hco_npi_mdm,ref_hco_state,ref_hco_zip,patient_id from zolg_master_v2
) select * from uni



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


@app.route('/hcp-360', methods=['GET'])
def fetch_hcp_360():
    """
    Fetch HCP 360 Data from AWS Athena based on hcp_name or ref_npi
    """
    hcp_name = request.args.get('hcp_name')
    ref_npi = request.args.get('ref_npi')

    query = """
    SELECT DISTINCT hcp_id, zolg_prescriber, zolgensma_iv_target, kol, patient_id, 
                    drug_name, age_group, final_spec, hcp_segment, hcp_name, 
                    hco_mdm_name,
                    COALESCE(
                        CONCAT(
                            COALESCE(hco_addr_line_1, ''), ', ',
                            COALESCE(hco_city, ''), ', ',
                            COALESCE(hco_state, ''), ', ',
                            COALESCE(hco_postal_cd_prim, '')
                        ), ''
                    )  AS address, 
                    ref_npi, ref_name, congress_contributions, publications, clinical_trials
    FROM "product_landing"."zolg_master_v2"
    WHERE 1=1
    """

    # Add filtering conditions dynamically
    filters = []
    if hcp_name:
        filters.append(f"hcp_name = '{hcp_name}'")
    if ref_npi:
        filters.append(f"ref_npi = '{ref_npi}'")

    if filters:
        query += " AND " + " AND ".join(filters)

    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))



@app.route('/hco-360', methods=['GET'])
def fetch_hco_360():
    """
    Fetch HCP 360 Data from AWS Athena based on hcp_name, ref_npi, or hco_mdm_name
    """
    hcp_name = request.args.get('hcp_name')
    ref_npi = request.args.get('ref_npi')
    hco_mdm = request.args.get('hco_mdm')

    query = """
    SELECT DISTINCT hcp_id, zolg_prescriber, zolgensma_iv_target, kol, patient_id, 
                    drug_name, age_group, final_spec, hcp_segment, hcp_name, hco_mdm,
                    hco_mdm_name,
                    COALESCE(
                        CONCAT(
                            COALESCE(hco_addr_line_1, ''), ', ',
                            COALESCE(hco_city, ''), ', ',
                            COALESCE(hco_state, ''), ', ',
                            COALESCE(hco_postal_cd_prim, '')
                        ), ''
                    ) AS address, 
                    ref_npi, ref_name, congress_contributions, publications, clinical_trials, hco_grouping,hco_mdm_tier,account_setting_type,
ref_hco_npi_mdm
    FROM "product_landing"."zolg_master_v2"
    WHERE 1=1
    """

    # Dynamically add filters
    filters = []
    if hcp_name:
        filters.append(f"hcp_name = '{hcp_name}'")
    if ref_npi:
        filters.append(f"ref_npi = '{ref_npi}'")
    if hco_mdm:
        filters.append(f"hco_mdm = '{hco_mdm}'")

    if filters:
        query += " AND " + " AND ".join(filters)

    df = get_athena_data(query)  
    return jsonify(df.to_dict(orient='records'))


@app.route('/fetch-hcolandscape-kpicard', methods=['GET'])
def fetch_hcolandscape_kpicard():
    """
    Fetch KPI Card Data for HCO
    """
    query = """
   
        SELECT 
        COUNT(DISTINCT hco_mdm) AS hco_count,

        -- Avg patients per HCO
        (
            SELECT AVG(patient_count) AS avg_patients_per_hco
            FROM (
            SELECT 
                hco_mdm, 
                COUNT(DISTINCT patient_id) AS patient_count
            FROM "product_landing"."zolg_master"
            GROUP BY hco_mdm
            )
        ) AS avg_patients_per_hco,

        -- Total unique patients in last 12 months
        (
            SELECT COUNT(DISTINCT patient_id) AS total_patient_count
            FROM "product_landing"."zolg_master"
            WHERE DATE_PARSE(month, '%d-%m-%Y') >= DATE_ADD('month', -12, CURRENT_DATE)
        ) AS pt_12_mth,

        -- Count of non-null, non-dash ref_npi values
        (
            SELECT COUNT(DISTINCT ref_hco_npi_mdm)
            FROM "product_landing"."zolg_master"
            WHERE ref_hco_npi_mdm != '-'
        ) AS ref_hco_npi_mdm,
        (
            SELECT COUNT(DISTINCT patient_id) AS total_patient_count
            FROM "product_landing"."zolg_master"
            WHERE DATE_PARSE(month, '%d-%m-%Y') >= DATE_ADD('month', -12, CURRENT_DATE) and 
                ref_hco_npi_mdm !='-'
        ) AS ref_pt_12_mth


        FROM "product_landing"."zolg_master"
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))


@app.route('/fetch-hcolandscape-quater', methods=['GET'])
def fetch_hcoquarterdata():
    """
    Fetch KPI Card Data for HCO
    """
    query = """
      SELECT QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
           COUNT(DISTINCT patient_id) AS patient_count
    FROM "product_landing"."zolg_master_v2"
    WHERE YEAR(DATE_PARSE(month, '%d-%m-%Y')) = 2024 and hco_mdm!='-'
    GROUP BY QUARTER(DATE_PARSE(month, '%d-%m-%Y'))
    ORDER BY quarter DESC;
    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))


if __name__ == '__main__':
    app.run(debug=True)
