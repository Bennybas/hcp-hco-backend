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

from flask import request

@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    """
    Fetch Data from AWS Athena based on hcp_name
    """
    hcp_name = request.args.get('hcp_name')  
    
    query = f"""
    SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
           hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
           hcp_state, hco_state ,ref_hco_npi_mdm,ref_hcp_state,
ref_hco_state
    FROM "product_landing"."zolg_master"
    WHERE hcp_name = '{hcp_name}'
    """ if hcp_name else """
    SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
           hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
           hcp_state, hco_state ,ref_hco_npi_mdm,ref_hcp_state,ref_hco_state
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
select distinct hcp_id,hcp_state,hcp_zip,hco_mdm,hco_state,hco_postal_cd_prim,patient_id,
hco_postal_cd_prim,rend_hco_lat,rend_hco_long,hco_mdm_name from zolg_master_v2
union all 
select distinct ref_npi,ref_hcp_state,ref_hcp_zip,ref_hco_npi_mdm,ref_hco_state,ref_hco_zip,patient_id,
ref_hco_zip,ref_hco_lat,ref_hco_long,ref_organization_mdm_name
 from zolg_master_v2
) select * from uni

    """
    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))


@app.route('/fetch-hcplandscape', methods=['GET'])
def fetch_hcplandscape():
    """
    Fetch Map Data from AWS Athena with optional filters: year, age_group, and drug_name
    """
    year = request.args.get('year')
    age = request.args.get('age')
    drug = request.args.get('drug')

    filters = []

    if year and year.isdigit():
        filters.append(f"year = '{year}'")

    if age:
        filters.append(f"age_group = '{age}'")

    if drug:
        filters.append(f"drug_name = '{drug}'")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = f"""
        WITH a AS (
            SELECT DISTINCT 
                hcp_id AS rend_npi,
                hcp_name,
                ref_npi,
                ref_name,
                patient_id,
                QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
                SPLIT_PART(mth, '_', 1) AS year,
                drug_name,
                age_group,
                final_spec,
                hcp_segment,
                hco_mdm_name 
            FROM "product_landing"."zolg_master_v2"
        )
        SELECT DISTINCT * FROM a
        {where_clause}
    """

    df = get_athena_data(query)
    return jsonify(df.to_dict(orient='records'))

@app.route('/fetch-hcolandscape', methods=['GET'])
def fetch_hcolandscape():
    """
    Fetch HCO Landscape Data from AWS Athena with Filters
    """
    # Get filters from query parameters
    year = request.args.get('year')
    age_group = request.args.get('age_group')
    drug_name = request.args.get('drug_name')
    zolg_prescriber = request.args.get('zolg_prescriber')
    zolgensma_iv_target = request.args.get('zolgensma_iv_target')
    kol = request.args.get('kol')
    hcp_segment = request.args.get('hcp_segment')
    hco_state = request.args.get('hco_state')

    # Base query
    query = """
        with a as(
select distinct hco_mdm as rend_hco_npi,
hco_mdm_name,
ref_hco_npi_mdm,ref_organization_mdm_name,patient_id,QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,split_part(mth,'_',1) as year,
drug_name,age_group,zolg_prescriber,zolgensma_iv_target,kol,hco_mdm_tier,hco_grouping,
hco_state FROM "product_landing"."zolg_master_v2")
select distinct * from a 
        WHERE 1=1
    """

    # Append filters dynamically
    if year:
        query += f" AND year = '{year}'"
    if age_group:
        query += f" AND age_group = '{age_group}'"
    if drug_name:
        query += f" AND drug_name = '{drug_name}'"
    if zolg_prescriber:
        query += f" AND zolg_prescriber = '{zolg_prescriber}'"
    if zolgensma_iv_target:
        query += f" AND zolgensma_iv_target = '{zolgensma_iv_target}'"
    if kol:
        query += f" AND kol = '{kol}'"
    if hcp_segment:
        query += f" AND hcp_segment = '{hcp_segment}'"
    if hco_state:
        query += f" AND hco_state = '{hco_state}'"

    # Run the query
    df = get_athena_data(query)

    # Return the result
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
    ref_hco_npi_mdm = request.args.get('ref_hco_npi_mdm')

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
    if ref_hco_npi_mdm:
        filters.append(f"ref_hco_npi_mdm ='{ref_hco_npi_mdm}'")

    if filters:
        query += " AND " + " AND ".join(filters)

    df = get_athena_data(query)  
    return jsonify(df.to_dict(orient='records'))





if __name__ == '__main__':
    app.run(debug=True)
