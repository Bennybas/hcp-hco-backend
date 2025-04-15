from flask import Flask, jsonify, request
from pyathena import connect
import pandas as pd
from flasgger import Swagger
from flask_cors import CORS
from dotenv import load_dotenv
import os
import time
import threading
import logging
import queue
import concurrent.futures
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)
swagger = Swagger(app)

# Request-level cache with timestamps
data_cache = {}
# Cache lock for thread safety
cache_lock = threading.RLock()
# Cache duration in seconds (Default: 1 hour)
CACHE_DURATION = int(os.getenv("CACHE_DURATION", "3600"))
# Refresh interval in seconds (Default: 55 minutes - slightly less than cache duration)
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", str(CACHE_DURATION - 300)))
# Thread pool for executing queries
executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
# Queue for background refresh tasks
refresh_queue = queue.Queue()
# Flag to control background threads
running = True

def get_athena_data(query):
    """Connect to Athena and execute query"""
    try:
        conn = connect(
            aws_access_key_id=os.getenv("ATHENA_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("ATHENA_SECRET_KEY"),
            region_name=os.getenv("ATHENA_REGION"),
            s3_staging_dir=os.getenv("S3_STAGING_DIR"),
            schema_name=os.getenv("ATHENA_DATABASE")
        )
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logger.error(f"Error executing Athena query: {str(e)}")
        logger.error(f"Query: {query}")
        raise

def cached_query(cache_key, query_func):
    """
    Check cache, optionally refresh, run query_func if needed, cache results
    """
    refresh = request.args.get("refresh", "false").lower() == "true"
    current_time = time.time()
    
    # Thread-safe cache access
    with cache_lock:
        # Check if data exists in cache and is not expired
        if not refresh and cache_key in data_cache:
            cached_time, cached_data = data_cache[cache_key]
            # Check if cache is still valid
            if current_time - cached_time < CACHE_DURATION:
                logger.info(f"Using cached data for {cache_key}")
                return cached_data
    
    # Data not in cache, expired, or refresh requested - execute query
    logger.info(f"Executing query for {cache_key}")
    try:
        result = query_func()
        
        # Store result in cache with timestamp
        with cache_lock:
            data_cache[cache_key] = (current_time, result)
            
        # Add to refresh queue for future background refresh
        refresh_queue.put((cache_key, query_func))
        
        return result
    except Exception as e:
        logger.error(f"Error in cached_query for {cache_key}: {str(e)}")
        # If we have stale data, return it rather than failing
        with cache_lock:
            if cache_key in data_cache:
                logger.warning(f"Returning stale data for {cache_key} due to query error")
                return data_cache[cache_key][1]
        # No cached data available, re-raise the exception
        raise

def background_refresh_worker():
    """Worker thread that refreshes cache entries before they expire"""
    logger.info("Starting background refresh worker")
    
    # Keep track of when each key should be refreshed next
    refresh_schedule = {}
    
    while running:
        try:
            # Check if there are new items in the queue
            try:
                # Non-blocking queue check
                cache_key, query_func = refresh_queue.get(block=False)
                # Schedule this key for refresh at appropriate time
                next_refresh = time.time() + REFRESH_INTERVAL
                refresh_schedule[cache_key] = (next_refresh, query_func)
                logger.info(f"Scheduled {cache_key} for refresh at {datetime.fromtimestamp(next_refresh)}")
                refresh_queue.task_done()
            except queue.Empty:
                # No new items, continue with scheduled refreshes
                pass
            
            # Check if any scheduled refreshes are due
            current_time = time.time()
            keys_to_refresh = []
            
            for key, (refresh_time, func) in refresh_schedule.items():
                if current_time >= refresh_time:
                    keys_to_refresh.append((key, func))
            
            # Remove the keys we're about to refresh from the schedule
            for key, _ in keys_to_refresh:
                del refresh_schedule[key]
            
            # Submit refresh tasks to thread pool
            for key, func in keys_to_refresh:
                executor.submit(refresh_cache_entry, key, func)
            
            # Sleep briefly to avoid busy waiting
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error in background refresh worker: {str(e)}")
            time.sleep(30)  # Sleep longer on error

def refresh_cache_entry(cache_key, query_func):
    """Refresh a single cache entry"""
    try:
        logger.info(f"Background refresh for {cache_key}")
        result = query_func()
        current_time = time.time()
        
        with cache_lock:
            data_cache[cache_key] = (current_time, result)
        
        # Re-add to refresh queue for next cycle
        next_refresh = time.time() + REFRESH_INTERVAL
        refresh_schedule = {cache_key: (next_refresh, query_func)}
        logger.info(f"Successfully refreshed {cache_key}, next refresh at {datetime.fromtimestamp(next_refresh)}")
        
    except Exception as e:
        logger.error(f"Error refreshing cache for {cache_key}: {str(e)}")
        # Schedule a retry sooner than normal
        next_refresh = time.time() + 300  # 5 minutes
        refresh_schedule = {cache_key: (next_refresh, query_func)}
        logger.info(f"Will retry {cache_key} at {datetime.fromtimestamp(next_refresh)}")

def preload_common_queries():
    """Preload the most commonly used queries into cache on startup"""
    logger.info("Preloading common queries into cache")
    
    # List of common queries to preload
    common_queries = [
        ("fetch-map-data", lambda: fetch_map_data_query()),
        ("fetch-data-all", lambda: fetch_data_query(None)),
        ("hcplandscape-all-all-all", lambda: fetch_hcplandscape_query(None, None, None)),
        ("hcolandscape-all-all-all-all-all-all-all-all", lambda: fetch_hcolandscape_query({}))
    ]
    
    # Submit each query to the thread pool
    futures = []
    for key, func in common_queries:
        futures.append(executor.submit(preload_cache_entry, key, func))
    
    # Wait for all preload tasks to complete
    for future in concurrent.futures.as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logger.error(f"Error in preload task: {str(e)}")

def preload_cache_entry(cache_key, query_func):
    """Preload a single cache entry"""
    try:
        logger.info(f"Preloading {cache_key}")
        result = query_func()
        current_time = time.time()
        
        with cache_lock:
            data_cache[cache_key] = (current_time, result)
        
        # Add to refresh queue for future background refresh
        refresh_queue.put((cache_key, query_func))
        logger.info(f"Successfully preloaded {cache_key}")
        
    except Exception as e:
        logger.error(f"Error preloading cache for {cache_key}: {str(e)}")

# Query function implementations
def fetch_data_query(hcp_name):
    if hcp_name:
        q = f"""
        SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
               hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
               hcp_state, hco_state, ref_hco_npi_mdm, ref_hcp_state, ref_hco_state,
               final_spec, hco_grouping, zolgensma_iv_target, 
               SPLIT_PART(mth,'_',1) AS year, rend_hco_territory, ref_hco_territory
        FROM "product_landing"."zolg_master_v3" 
        WHERE hcp_name = '{hcp_name}'
        """
    else:
        q = """
        SELECT DISTINCT hcp_id, zolg_prescriber, patient_id, drug_name, hcp_name, 
               hco_mdm, hco_mdm_name, hco_mdm_tier, hcp_segment, ref_npi, 
               hcp_state, hco_state, ref_hco_npi_mdm, ref_hcp_state, ref_hco_state,
               final_spec, hco_grouping, zolgensma_iv_target, 
               SPLIT_PART(mth,'_',1) AS year, rend_hco_territory, ref_hco_territory
        FROM "product_landing"."zolg_master_v3"
        """
    df = get_athena_data(q)
    return df.to_dict(orient='records')

def fetch_map_data_query():
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
    return df.to_dict(orient='records')

def fetch_hcplandscape_query(year, age, drug):
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
    return df.to_dict(orient='records')

def fetch_hcolandscape_query(params):
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
    return df.to_dict(orient='records')

# Route handlers
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
    cache_key = f"fetch-data-{hcp_name or 'all'}"
    
    data = cached_query(cache_key, lambda: fetch_data_query(hcp_name))
    return jsonify(data)

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
    cache_key = "fetch-map-data"
    
    data = cached_query(cache_key, fetch_map_data_query)
    return jsonify(data)

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
    # Create cache key based on filters
    cache_key = f"hcplandscape-{year or 'all'}-{age or 'all'}-{drug or 'all'}"
    
    data = cached_query(cache_key, lambda: fetch_hcplandscape_query(year, age, drug))
    return jsonify(data)

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
    key_parts = [params[k] or 'all' for k in params]
    cache_key = "hcolandscape-" + "-".join(key_parts)
    
    data = cached_query(cache_key, lambda: fetch_hcolandscape_query(params))
    return jsonify(data)

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
    
    def query_func():
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
    
    data = cached_query(cache_key, query_func)
    return jsonify(data)

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
    
    def query_func():
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
    
    data = cached_query(cache_key, query_func)
    return jsonify(data)

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
    
    def query_func():
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
    
    data = cached_query(cache_key, query_func)
    return jsonify(data)

# Health check endpoint
@app.route('/health', methods=['GET'])
def health():
    """Simple health check endpoint"""
    return jsonify({
        "status": "ok", 
        "cache_entries": len(data_cache),
        "refresh_queue_size": refresh_queue.qsize()
    })

# Status endpoint to see cache status
@app.route('/cache-status', methods=['GET'])
def cache_status():
    """View cache status"""
    cache_info = {}
    current_time = time.time()
    
    with cache_lock:
        for key, (timestamp, _) in data_cache.items():
            age = current_time - timestamp
            expiry = CACHE_DURATION - age
            cache_info[key] = {
                "age_seconds": round(age),
                "expires_in_seconds": round(expiry),
                "is_fresh": expiry > 0
            }
    
    return jsonify({
        "cache_entries": len(data_cache),
        "cache_duration_seconds": CACHE_DURATION,
        "refresh_interval_seconds": REFRESH_INTERVAL,
        "refresh_queue_size": refresh_queue.qsize(),
        "entries": cache_info
    })

# Clear cache endpoint
@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    """Clear all cache entries"""
    with cache_lock:
        data_cache.clear()
    return jsonify({"status": "success", "message": "Cache cleared"})

# Initialize background threads
def start_background_threads():
    # Start the background refresh worker
    refresh_thread = threading.Thread(target=background_refresh_worker, daemon=True)
    refresh_thread.start()
    
    # Preload common queries
    preload_thread = threading.Thread(target=preload_common_queries, daemon=True)
    preload_thread.start()
    
    return refresh_thread, preload_thread

def shutdown_threads():
    global running
    running = False
    logger.info("Shutting down background threads...")
    executor.shutdown(wait=True)
    logger.info("Thread pool shutdown complete")

if __name__ == '__main__':
    refresh_thread, preload_thread = start_background_threads()
    
    try:
        app.run(debug=False, host='0.0.0.0', port=int(os.getenv("PORT", "5000")))
    finally:
        shutdown_threads()