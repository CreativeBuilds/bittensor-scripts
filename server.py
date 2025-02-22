import os
import time
import datetime
import decimal
from functools import wraps

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

# Create a global database connection and cursor (dictionary cursor for ease-of-use)
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
        # Generate a unique cache key: client IP + full URL (path + query)
        key = f"{request.client.host}:{request.url.path}?{request.url.query}"
        now = time.time()
        if key in response_cache:
            cached = response_cache[key]
            if now - cached["timestamp"] < 10:
                return JSONResponse(content=cached["data"])
        # Call the actual endpoint function
        result = await func(request, *args, **kwargs)
        # Serialize result before caching
        serialized_result = serialize_data(result)
        response_cache[key] = {"data": serialized_result, "timestamp": time.time()}
        return JSONResponse(content=serialized_result)
    return wrapper

@app.get("/snapshots/latest")
@cache_rate_limit
async def get_latest_snapshot(request: Request):
    """
    Returns the most recent snapshot along with its subnet records.
    """
    # Get the latest snapshot (order by timestamp descending)
    db_cursor.execute("SELECT * FROM subnet_snapshots ORDER BY snapshot_timestamp DESC LIMIT 1")
    snapshot = db_cursor.fetchone()
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshots found")
    snapshot_id = snapshot["snapshot_id"]
    # Retrieve associated subnet records
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
    """
    # Start with a base query
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
    results = db_cursor.fetchall()
    return results

if __name__ == "__main__":
    import uvicorn
    # Run the FastAPI app with uvicorn on host 0.0.0.0:8000
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
