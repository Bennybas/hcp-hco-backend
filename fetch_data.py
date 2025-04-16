from flask import Flask, jsonify, request
from pyathena import connect
import pandas as pd
from flasgger import Swagger
from flask_cors import CORS
from dotenv import load_dotenv
import os
import time
import threading
import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)
CORS(app)
swagger = Swagger(app)

# Global cache storage
data_cache = {}
# Cache refresh interval in seconds (1 hour)
CACHE_REFRESH_INTERVAL = 3600

def get_athena_data(query):
    """Execute query on AWS Athena and return results as DataFrame"""
    conn = connect(
        aws_access_key_id=os.getenv("ATHENA_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ATHENA_SECRET_KEY"),
        region_name=os.getenv("ATHENA_REGION"),
        s3_staging_dir=os.getenv("S3_STAGING_DIR"),
        schema_name=os.getenv("ATHENA_DATABASE")
    )
    df = pd.read_sql(query, conn)
    return df

def cached_jsonify(cache_key, query_fn):
    """
    Helper: check cache, optionally refresh, run query_fn() if needed, cache & return JSON.
    Used for non-cached endpoints.
    """
    refresh = request.args.get("refresh", "false").lower() == "true"
    if cache_key in data_cache and not refresh:
        return jsonify(data_cache[cache_key])

    # run the provided query function
    records = query_fn()
    data_cache[cache_key] = records
    return jsonify(records)

def update_cache():
    """Update all cache entries for the main APIs"""
    logger.info("Starting cache refresh at %s", datetime.datetime.now())
    
    # Refresh fetch-data for all HCPs (no specific HCP name)
    try:
        q = """
        SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
               hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
               hcp_state, hco_state, ref_hco_npi_mdm, ref_hcp_state, ref_hco_state,final_spec,hco_grouping,zolgensma_iv_target,SPLIT_PART(mth,'_',1) AS year,rend_hco_territory,ref_hco_territory
        FROM "product_landing"."zolg_master_v3"
        """
        df = get_athena_data(q)
        data_cache["fetch-data-all"] = df.to_dict(orient='records')
        logger.info("Updated cache for fetch-data-all")
    except Exception as e:
        logger.error("Error updating fetch-data-all: %s", str(e))
    
    # Refresh fetch-map-data
    try:
        q = """
        WITH uni AS (
          SELECT DISTINCT hcp_id, hcp_state, hcp_zip, hco_mdm, hco_state,
                          hco_postal_cd_prim, patient_id, hco_postal_cd_prim,
                          rend_hco_lat, rend_hco_long, hco_mdm_name,hco_grouping,rend_hco_territory,SPLIT_PART(mth,'_',1) AS year,hcp_segment
        FROM zolg_master_v3 
          UNION ALL
          SELECT DISTINCT ref_npi AS hcp_id, ref_hcp_state AS hcp_state,
                          ref_hcp_zip AS hcp_zip, ref_hco_npi_mdm AS hco_mdm,
                          ref_hco_state AS hco_state, ref_hco_zip AS hco_postal_cd_prim,
                          patient_id, ref_hco_zip AS hco_postal_cd_prim,
                          ref_hco_lat AS rend_hco_lat, ref_hco_long AS rend_hco_long,
                          ref_organization_mdm_name AS hco_mdm_name,hco_grouping,ref_hco_territory,SPLIT_PART(mth,'_',1) AS year,hcp_segment
          FROM zolg_master_v3
        )
        SELECT * FROM uni
        """
        df = get_athena_data(q)
        data_cache["fetch-map-data"] = df.to_dict(orient='records')
        logger.info("Updated cache for fetch-map-data")
    except Exception as e:
        logger.error("Error updating fetch-map-data: %s", str(e))
    
    # Refresh fetch-hcplandscape (default with no filters)
    try:
        q = """
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
            hco_mdm_name ,ref_hcp_state,hcp_state,zolgensma_naive
          FROM "product_landing"."zolg_master_v4"
        )
        SELECT DISTINCT * FROM a
        """
        df = get_athena_data(q)
        data_cache["hcplandscape-all-all-all"] = df.to_dict(orient='records')
        logger.info("Updated cache for hcplandscape-all-all-all")
    except Exception as e:
        logger.error("Error updating hcplandscape: %s", str(e))
    
    # Refresh fetch-hcolandscape (default with no filters)
    try:
        q = """
        WITH a AS (
          SELECT DISTINCT
            hco_mdm AS rend_hco_npi,
            hco_mdm_name,
            ref_hco_npi_mdm,
            ref_organization_mdm_name,
            patient_id,
            QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
            SPLIT_PART(mth,'_',1) AS year,
            drug_name,
            age_group,
            zolg_prescriber,
            zolgensma_iv_target,
            kol,
            hco_mdm_tier,
            hco_grouping,
            hco_state,zolgensma_naive,hcp_id
          FROM "product_landing"."zolg_master_v4"
        )
        SELECT DISTINCT * FROM a
        WHERE 1=1
        """
        df = get_athena_data(q)
        data_cache["hcolandscape-all-all-all-all-all-all-all-all"] = df.to_dict(orient='records')
        logger.info("Updated cache for hcolandscape (default)")
    except Exception as e:
        logger.error("Error updating hcolandscape: %s", str(e))
    
    logger.info("Completed cache refresh at %s", datetime.datetime.now())

def scheduled_cache_update():
    """Function to run the cache update on a schedule"""
    while True:
        # Update all caches
        update_cache()
        # Sleep for the cache refresh interval
        time.sleep(CACHE_REFRESH_INTERVAL)

# Start the cache update thread when the app is initialized
with app.app_context():
    # Immediate first cache update
    update_cache()
    
    # Start the periodic cache update in a separate thread
    thread = threading.Thread(target=scheduled_cache_update)
    thread.daemon = True  # This ensures the thread will exit when the main process exits
    thread.start()
    logger.info("Cache update scheduler started")

@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    """
    Fetch Data from AWS Athena based on hcp_name
    ---
    parameters:
      - name: hcp_name
        in: query
        type: string
      - name: refresh
        in: query
        type: boolean
    """
    hcp_name = request.args.get('hcp_name')
    refresh = request.args.get("refresh", "false").lower() == "true"
    
    cache_key = f"fetch-data-{hcp_name or 'all'}"
    
    # If specific HCP requested and not in cache, or refresh requested
    if (hcp_name and (cache_key not in data_cache or refresh)):
        q = f"""
        SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
               hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
               hcp_state, hco_state, ref_hco_npi_mdm, ref_hcp_state, ref_hco_state,final_spec,hco_grouping,zolgensma_iv_target,SPLIT_PART(mth,'_',1) AS year,rend_hco_territory,ref_hco_territory
        FROM "product_landing"."zolg_master_v3"
        WHERE hcp_name = '{hcp_name}'
        """
        df = get_athena_data(q)
        data_cache[cache_key] = df.to_dict(orient='records')
    
    # Return from cache if it exists, otherwise return empty list
    if cache_key in data_cache:
        return jsonify(data_cache[cache_key])
    else:
        return jsonify([])

@app.route('/fetch-map-data', methods=['GET'])
def fetch_map_data():
    """
    Fetch Map Data from AWS Athena
    ---
    parameters:
      - name: refresh
        in: query
        type: boolean
    """
    refresh = request.args.get("refresh", "false").lower() == "true"
    cache_key = "fetch-map-data"
    
    # If refresh requested or not in cache, run the query
    if refresh or cache_key not in data_cache:
        # This should normally not happen as the cache is refreshed periodically
        update_cache()
    
    # Return from cache if it exists, otherwise return empty list
    if cache_key in data_cache:
        return jsonify(data_cache[cache_key])
    else:
        return jsonify([])

@app.route('/fetch-hcplandscape', methods=['GET'])
def fetch_hcplandscape():
    """
    Fetch HCP Landscape Data with optional filters: year, age, drug
    ---
    parameters:
      - name: year
        in: query
        type: string
      - name: age
        in: query
        type: string
      - name: drug
        in: query
        type: string
      - name: refresh
        in: query
        type: boolean
    """
    year = request.args.get('year')
    age = request.args.get('age')
    drug = request.args.get('drug')
    refresh = request.args.get("refresh", "false").lower() == "true"
    
    cache_key = f"hcplandscape-{year or 'all'}-{age or 'all'}-{drug or 'all'}"
    
    # If specific filters requested and not in cache, or refresh requested
    if (year or age or drug) and (cache_key not in data_cache or refresh):
        filters = []
        if year and year.isdigit():
            filters.append(f"year = '{year}'")
        if age:
            filters.append(f"age_group = '{age}'")
        if drug:
            filters.append(f"drug_name = '{drug}'")
        where = f"WHERE {' AND '.join(filters)}" if filters else ""

        q = f"""
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
            hco_mdm_name ,ref_hcp_state,hcp_state,zolgensma_naive
          FROM "product_landing"."zolg_master_v4"
        )
        SELECT DISTINCT * FROM a
        {where}
        """
        df = get_athena_data(q)
        data_cache[cache_key] = df.to_dict(orient='records')
    
    # Return from cache if it exists, otherwise return empty list
    if cache_key in data_cache:
        return jsonify(data_cache[cache_key])
    else:
        # Try to return the default (all) data if specific cache not found
        if "hcplandscape-all-all-all" in data_cache:
            return jsonify(data_cache["hcplandscape-all-all-all"])
        else:
            return jsonify([])

@app.route('/fetch-hcolandscape', methods=['GET'])
def fetch_hcolandscape():
    """
    Fetch HCO Landscape Data with optional filters
    ---
    parameters:
      - name: year
        in: query
        type: string
      - name: age_group
        in: query
        type: string
      - name: drug_name
        in: query
        type: string
      - name: zolg_prescriber
        in: query
        type: string
      - name: zolgensma_iv_target
        in: query
        type: string
      - name: kol
        in: query
        type: string
      - name: hcp_segment
        in: query
        type: string
      - name: hco_state
        in: query
        type: string
      - name: refresh
        in: query
        type: boolean
    """
    params = {k: request.args.get(k) for k in [
        'year','age_group','drug_name','zolg_prescriber',
        'zolgensma_iv_target','kol','hcp_segment','hco_state'
    ]}
    refresh = request.args.get("refresh", "false").lower() == "true"
    
    has_filters = any(params.values())
    key_parts = [params[k] or 'all' for k in params]
    cache_key = "hcolandscape-" + "-".join(key_parts)
    
    # If specific filters requested and not in cache, or refresh requested
    if has_filters and (cache_key not in data_cache or refresh):
        q = """
        WITH a AS (
          SELECT DISTINCT
            hco_mdm AS rend_hco_npi,
            hco_mdm_name,
            ref_hco_npi_mdm,
            ref_organization_mdm_name,
            patient_id,
            QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
            SPLIT_PART(mth,'_',1) AS year,
            drug_name,
            age_group,
            zolg_prescriber,
            zolgensma_iv_target,
            kol,
            hco_mdm_tier,
            hco_grouping,
            hco_state,zolgensma_naive,hcp_id
          FROM "product_landing"."zolg_master_v4"
        )
        SELECT DISTINCT * FROM a
        WHERE 1=1
        """
        for param, val in params.items():
            if val:
                q += f" AND {param} = '{val}'"
        df = get_athena_data(q)
        data_cache[cache_key] = df.to_dict(orient='records')
    
    # Return from cache if it exists, otherwise return empty list
    if cache_key in data_cache:
        return jsonify(data_cache[cache_key])
    else:
        # Try to return the default (all) data if specific cache not found
        default_key = "hcolandscape-all-all-all-all-all-all-all-all"
        if default_key in data_cache:
            return jsonify(data_cache[default_key])
        else:
            return jsonify([])

@app.route('/hcp-360', methods=['GET'])
def fetch_hcp_360():
    """
    Fetch HCP 360 Data based on hcp_name or ref_npi
    ---
    parameters:
      - name: hcp_name
        in: query
        type: string
      - name: ref_npi
        in: query
        type: string
      - name: refresh
        in: query
        type: boolean
    """
    hcp_name = request.args.get('hcp_name')
    ref_npi = request.args.get('ref_npi')
    cache_key = f"hcp360-{hcp_name or 'none'}-{ref_npi or 'none'}"

    def query_fn():
        q = """
        SELECT DISTINCT
          hcp_id, zolg_prescriber, zolgensma_iv_target, kol, patient_id,
          drug_name, age_group, final_spec, hcp_segment, hcp_name,
          hco_mdm_name,
          COALESCE(
            CONCAT(
              COALESCE(hco_addr_line_1,''), ', ',
              COALESCE(hco_city,''), ', ',
              COALESCE(hco_state,''), ', ',
              COALESCE(hco_postal_cd_prim,'')
            ), ''
          ) AS address,
          ref_npi, ref_name, congress_contributions, publications, clinical_trials,QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
            SPLIT_PART(mth,'_',1) AS year
        FROM "product_landing"."zolg_master_v2"
        WHERE 1=1
        """
        if hcp_name:
            q += f" AND hcp_name = '{hcp_name}'"
        if ref_npi:
            q += f" AND ref_npi = '{ref_npi}'"
        df = get_athena_data(q)
        return df.to_dict(orient='records')

    return cached_jsonify(cache_key, query_fn)

@app.route('/hco-360', methods=['GET'])
def fetch_hco_360():
    """
    Fetch HCO 360 Data based on various filters
    ---
    parameters:
      - name: hcp_name
        in: query
        type: string
      - name: ref_npi
        in: query
        type: string
      - name: hco_mdm
        in: query
        type: string
      - name: ref_hco_npi_mdm
        in: query
        type: string
      - name: refresh
        in: query
        type: boolean
    """
    params = {
        'hcp_name': request.args.get('hcp_name'),
        'ref_npi': request.args.get('ref_npi'),
        'hco_mdm': request.args.get('hco_mdm'),
        'ref_hco_npi_mdm': request.args.get('ref_hco_npi_mdm'),
    }
    key_parts = [params[k] or 'none' for k in params]
    cache_key = "hco360-" + "-".join(key_parts)

    def query_fn():
        q = """
        SELECT DISTINCT
          hcp_id, zolg_prescriber, zolgensma_iv_target, kol, patient_id,
          drug_name, age_group, final_spec, hcp_segment, hcp_name, hco_mdm,
          hco_mdm_name,
          COALESCE(
            CONCAT(
              COALESCE(hco_addr_line_1,''), ', ',
              COALESCE(hco_city,''), ', ',
              COALESCE(hco_state,''), ', ',
              COALESCE(hco_postal_cd_prim,'')
            ), ''
          ) AS address,
          ref_npi, ref_name, congress_contributions, publications, clinical_trials,
          hco_grouping, case when hco_mdm_tier='Tier 1' then 'HIGH'
          when hco_mdm_tier='Tier 2' then 'MEDIUM' when hco_mdm_tier='Tier 3' then 'LOW' when hco_mdm_tier='Tier 4' then 'V. LOW' else hco_mdm_tier end as hco_mdm_tier, account_setting_type, ref_hco_npi_mdm,QUARTER(DATE_PARSE(month, '%d-%m-%Y')) AS quarter,
            SPLIT_PART(mth,'_',1) AS year,within_outside_hco_referral
        FROM "product_landing"."zolg_master_v2"
        WHERE 1=1
        """
        for param, val in params.items():
            if val:
                q += f" AND {param} = '{val}'"
        df = get_athena_data(q)
        return df.to_dict(orient='records')

    return cached_jsonify(cache_key, query_fn)

@app.route('/fetch-refer-data', methods=['GET'])
def fetch_referal_data():
    """
    Fetch Referral Data from AWS Athena
    ---
    parameters:
      - name: refresh
        in: query
        type: boolean
    """
    cache_key = "fetch-refer-data"
    def query_fn():
        q = """
        SELECT DISTINCT
          patient_id, hcp_id, hcp_name, hcp_state, hcp_zip, hco_mdm, hco_state,
          hco_postal_cd_prim, rend_hco_lat, rend_hco_long, hco_mdm_name,
          ref_npi, ref_name, ref_hcp_state, ref_hcp_zip, ref_hco_npi_mdm,
          ref_hco_state, ref_hco_zip, ref_hco_lat, ref_hco_long,
          ref_organization_mdm_name, hcp_segment, final_spec,
          hco_grouping, hco_mdm_tier, SPLIT_PART(mth,'_',1) AS year,rend_hco_territory,ref_hco_territory
        FROM zolg_master_v3
        """
        df = get_athena_data(q)
        return df.to_dict(orient='records')

    return cached_jsonify(cache_key, query_fn)
if __name__ == '__main__':
    app.run(debug=True)