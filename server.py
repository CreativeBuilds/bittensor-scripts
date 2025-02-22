import os
import time
import datetime
import decimal
from functools import wraps
from collections import defaultdict
from datetime import timedelta

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import mysql.connector

# --- Load .env variables if present ---
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv()

# --- Database Configuration ---
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'liquidity'),
    'password': os.getenv('DB_PASSWORD', 'hackermanimin'),
    'database': os.getenv('DB_NAME', 'prices')
}

# Global database connection and cursor (using a dictionary cursor)
db_connection = mysql.connector.connect(**db_config)
db_cursor = db_connection.cursor(dictionary=True)

app = FastAPI()

# In-memory cache for rate limiting (key: IP+endpoint, value: {data, timestamp})
response_cache = {}

def serialize_data(data):
    """
    Recursively convert datetime and decimal objects to JSON serializable types.
    """
    if isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_data(item) for item in data]
    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()
    elif isinstance(data, decimal.Decimal):
        return float(data)
    else:
        return data

def cache_rate_limit(func):
    """
    Decorator that caches endpoint responses per IP and URL for 10 seconds.
    If the same client IP calls the same endpoint within 10 seconds,
    the cached response is returned.
    """
    @wraps(func)
    async def wrapper(request: Request, *args, **kwargs):
        key = f"{request.client.host}:{request.url.path}?{request.url.query}"
        now = time.time()
        if key in response_cache:
            cached = response_cache[key]
            if now - cached["timestamp"] < 10:
                return JSONResponse(content=cached["data"])
        result = await func(request, *args, **kwargs)
        serialized_result = serialize_data(result)
        response_cache[key] = {"data": serialized_result, "timestamp": time.time()}
        return JSONResponse(content=serialized_result)
    return wrapper

# ----- Analysis Helper Functions -----
def compute_ema(time_series, timeframe):
    """
    Compute the EMA for a given time_series (list of (timestamp, value))
    over the specified timeframe (in minutes).
    Returns a list of (timestamp, ema_value).
    """
    alpha = 2 / (timeframe + 1)
    ema = None
    ema_values = []
    for ts, value in time_series:
        if ema is None:
            ema = value
        else:
            ema = alpha * value + (1 - alpha) * ema
        ema_values.append((ts, ema))
    return ema_values

def compute_subnet_gap_trends(records):
    """
    For a given subnet’s records (assumed sorted by timestamp), compute the time series for:
      - 5m EMA for price (using records from last 5 minutes)
      - 60m EMA for price (using records from last 60 minutes)
      - 5m EMA for emission (using records from last 5 minutes)
      - 60m EMA for emission (using records from last 60 minutes)

    Then define the gap as: Gap(t) = EMA_5m(t) - EMA_60m(t)
    and compute the change (delta) in the gap over the 5-minute window 
    (delta = gap_end - gap_begin).

    Returns a dictionary with:
      - final_ema5_price, final_ema60_price, final_gap_price, delta_gap_price,
      - final_ema5_emission, final_ema60_emission, final_gap_emission, delta_gap_emission
    or None if insufficient data.
    """
    last_ts = records[-1]['snapshot_timestamp']
    window_5 = last_ts - timedelta(minutes=5)
    window_60 = last_ts - timedelta(minutes=60)
    
    series_5_price = [(rec['snapshot_timestamp'], float(rec['price']))
                      for rec in records if rec['snapshot_timestamp'] >= window_5]
    series_60_price = [(rec['snapshot_timestamp'], float(rec['price']))
                       for rec in records if rec['snapshot_timestamp'] >= window_60]
    series_5_emission = [(rec['snapshot_timestamp'], float(rec['emission']))
                         for rec in records if rec['snapshot_timestamp'] >= window_5]
    series_60_emission = [(rec['snapshot_timestamp'], float(rec['emission']))
                          for rec in records if rec['snapshot_timestamp'] >= window_60]
    
    # Need at least two points in the 5m series to measure a 5-minute delta
    if len(series_5_price) < 2 or len(series_60_price) < 1 or len(series_5_emission) < 2 or len(series_60_emission) < 1:
        return None
    
    ema5_price_series = compute_ema(series_5_price, 5)
    ema60_price_series = compute_ema(series_60_price, 60)
    ema5_emission_series = compute_ema(series_5_emission, 5)
    ema60_emission_series = compute_ema(series_60_emission, 60)
    
    gap_price_begin = ema5_price_series[0][1] - ema60_price_series[0][1]
    gap_price_end   = ema5_price_series[-1][1] - ema60_price_series[-1][1]
    delta_gap_price = gap_price_end - gap_price_begin

    gap_emission_begin = ema5_emission_series[0][1] - ema60_emission_series[0][1]
    gap_emission_end   = ema5_emission_series[-1][1] - ema60_emission_series[-1][1]
    delta_gap_emission = gap_emission_end - gap_emission_begin

    return {
        'final_ema5_price': ema5_price_series[-1][1],
        'final_ema60_price': ema60_price_series[-1][1],
        'final_gap_price': gap_price_end,
        'delta_gap_price': delta_gap_price,
        'final_ema5_emission': ema5_emission_series[-1][1],
        'final_ema60_emission': ema60_emission_series[-1][1],
        'final_gap_emission': gap_emission_end,
        'delta_gap_emission': delta_gap_emission
    }

def get_gap_analysis():
    """
    Compute overall analysis for subnet gap trends over the last 240 minutes.
    Returns a dictionary with:
      - total_price: the sum of the last-known price from each subnet
      - total_price_ema: the overall EMA of total price over the 240-minute window
      - final_gap_total_price, delta_gap_total_price: the 5m vs. 60m gap for total price
      - subnet_gap_trends: list of top 10 subnets by emission, each with 5m vs 60m gap data
    """
    window_minutes = 240
    # Get the latest snapshot timestamp
    db_cursor.execute("SELECT MAX(snapshot_timestamp) AS max_ts FROM subnet_snapshots")
    max_ts_result = db_cursor.fetchone()
    if not max_ts_result or not max_ts_result['max_ts']:
        return None
    max_ts_dt = max_ts_result['max_ts']
    lower_bound = max_ts_dt - timedelta(minutes=window_minutes)

    # Query all relevant subnet_records + snapshot timestamps
    query = """
    SELECT r.netuid, r.price, r.emission, s.snapshot_timestamp
    FROM subnet_records r
    JOIN subnet_snapshots s ON r.snapshot_id = s.snapshot_id
    WHERE s.snapshot_timestamp >= %s
    ORDER BY r.netuid, s.snapshot_timestamp
    """
    db_cursor.execute(query, (lower_bound,))
    records = db_cursor.fetchall()
    if not records:
        return None

    # Group by netuid, ignoring netuid=0
    groups = defaultdict(list)
    for rec in records:
        if rec['netuid'] == 0:
            continue
        groups[rec['netuid']].append(rec)

    # Compute the sum of each subnet’s latest price => total_price
    total_price = sum(float(recs[-1]['price']) for netuid, recs in groups.items() if recs)

    # Build a global time series for total price => (timestamp, total_price_at_that_ts)
    total_by_ts = {}
    for rec in records:
        if rec['netuid'] == 0:
            continue
        ts = rec['snapshot_timestamp']
        total_by_ts.setdefault(ts, 0.0)
        total_by_ts[ts] += float(rec['price'])

    # Sort by timestamp, compute an EMA over the entire 240-min window
    time_series = sorted(total_by_ts.items())  # list of (ts, total_price_at_ts)
    ema_total = None
    alpha = 2 / (window_minutes + 1)
    for ts, tp in time_series:
        if ema_total is None:
            ema_total = tp
        else:
            ema_total = alpha * tp + (1 - alpha) * ema_total

    # ---- Now compute a 5m vs. 60m gap for the *global* total price ----
    # We want the last 5 min and last 60 min from the global time_series.
    window_5 = max_ts_dt - timedelta(minutes=5)
    window_60 = max_ts_dt - timedelta(minutes=60)

    time_series_5 = [(ts, val) for (ts, val) in time_series if ts >= window_5]
    time_series_60 = [(ts, val) for (ts, val) in time_series if ts >= window_60]

    final_gap_total_price = None
    delta_gap_total_price = None

    # We need at least two points in the 5-minute slice to measure a 5-minute delta
    if len(time_series_5) >= 2 and len(time_series_60) >= 1:
        ema5_series = compute_ema(time_series_5, 5)
        ema60_series = compute_ema(time_series_60, 60)
        gap_begin = ema5_series[0][1] - ema60_series[0][1]
        gap_end = ema5_series[-1][1] - ema60_series[-1][1]
        final_gap_total_price = gap_end
        delta_gap_total_price = gap_end - gap_begin

    # ---- Subnet-level 5m vs 60m gap analysis ----
    subnet_results = []
    for netuid, recs in groups.items():
        recs.sort(key=lambda r: r['snapshot_timestamp'])
        gap_info = compute_subnet_gap_trends(recs)
        if gap_info is None:
            continue
        current_emission = float(recs[-1]['emission'])
        subnet_results.append({
            'netuid': netuid,
            'current_emission': current_emission,
            'final_ema5_price': gap_info['final_ema5_price'],
            'final_ema60_price': gap_info['final_ema60_price'],
            'final_gap_price': gap_info['final_gap_price'],
            'delta_gap_price': gap_info['delta_gap_price'],
            'final_ema5_emission': gap_info['final_ema5_emission'],
            'final_ema60_emission': gap_info['final_ema60_emission'],
            'final_gap_emission': gap_info['final_gap_emission'],
            'delta_gap_emission': gap_info['delta_gap_emission']
        })

    # Filter to top 10 subnets by current emission (highest first), then sort by netuid
    subnet_results.sort(key=lambda x: x['current_emission'], reverse=True)
    top_subnets = subnet_results[:10]
    top_subnets.sort(key=lambda x: x['netuid'])

    return {
        'total_price': total_price,
        'total_price_ema': ema_total,
        'final_gap_total_price': final_gap_total_price,      # <-- global 5m vs 60m gap
        'delta_gap_total_price': delta_gap_total_price,      # <-- global gap 5m delta
        'subnet_gap_trends': top_subnets
    }

# ----- Existing Endpoints -----
@app.get("/snapshots/latest")
@cache_rate_limit
async def get_latest_snapshot(request: Request):
    """
    Returns the most recent snapshot along with its subnet records.
    """
    db_cursor.execute("SELECT * FROM subnet_snapshots ORDER BY snapshot_timestamp DESC LIMIT 1")
    snapshot = db_cursor.fetchone()
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshots found")
    snapshot_id = snapshot["snapshot_id"]
    db_cursor.execute("SELECT * FROM subnet_records WHERE snapshot_id = %s", (snapshot_id,))
    records = db_cursor.fetchall()
    snapshot["records"] = records
    return snapshot

@app.get("/snapshots/{snapshot_id}")
@cache_rate_limit
async def get_snapshot(request: Request, snapshot_id: int):
    """
    Returns a specific snapshot (by ID) along with its subnet records.
    """
    db_cursor.execute("SELECT * FROM subnet_snapshots WHERE snapshot_id = %s", (snapshot_id,))
    snapshot = db_cursor.fetchone()
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    db_cursor.execute("SELECT * FROM subnet_records WHERE snapshot_id = %s", (snapshot_id,))
    records = db_cursor.fetchall()
    snapshot["records"] = records
    return snapshot

@app.get("/subnets")
@cache_rate_limit
async def get_subnets(request: Request, netuid: int = None, price: float = None, emission: float = None):
    """
    Returns subnet records, optionally filtered by netuid, price, and emission.
    Also returns analysis computed over the past 240 minutes.
    """
    # Raw filtered query (same as before)
    query = "SELECT * FROM subnet_records WHERE 1=1"
    params = []
    if netuid is not None:
        query += " AND netuid = %s"
        params.append(netuid)
    if price is not None:
        query += " AND price = %s"
        params.append(price)
    if emission is not None:
        query += " AND emission = %s"
        params.append(emission)
    db_cursor.execute(query, tuple(params))
    records = db_cursor.fetchall()

    # Compute additional analysis over a 240 minute window
    analysis = get_gap_analysis()

    return {
        "records": records,
        "analysis": analysis
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=7272, reload=True)
