import os
import math
import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict
from scipy.signal import savgol_filter  # For smoothing the acceleration vector
import numpy as np

# ANSI escape sequences for colors.
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

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
      - final_ema5_price, final_ema60_price, final_gap_price
      - delta_gap_price (change in price gap over 5 minutes)
      - final_ema5_emission, final_ema60_emission, final_gap_emission
      - delta_gap_emission (change in emission gap over 5 minutes)
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
    
    # We need at least two points in the 5m series to measure a change.
    if len(series_5_price) < 2 or len(series_60_price) < 1 or len(series_5_emission) < 2 or len(series_60_emission) < 1:
        return None
    
    ema5_price_series = compute_ema(series_5_price, 5)
    ema60_price_series = compute_ema(series_60_price, 60)
    ema5_emission_series = compute_ema(series_5_emission, 5)
    ema60_emission_series = compute_ema(series_60_emission, 60)
    
    # Use the first and last values of each series to approximate the gap change over 5 minutes.
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

def display_subnet_ema_gap_table(results):
    """
    Display a table comparing the 5m vs 60m EMA gap trends for each subnet.
    For price and emission, we show the final gap and the delta (change in gap over the last 5 minutes).
    A negative delta means the gap is shrinking (convergence), while a positive delta means it is widening (divergence).
    """
    header = (
        f"{'netuid':>6} | {'EMA_5 Price':>12} | {'EMA_60 Price':>12} | {'Gap Price':>10} | {'Δ Gap Price':>12} | {'Trend':>10} || "
        f"{'EMA_5 Emission':>14} | {'EMA_60 Emission':>14} | {'Gap Emission':>12} | {'Δ Gap Emission':>16} | {'Trend':>10}"
    )
    sep = "-" * len(header)
    print("\nSubnet 5m vs 60m EMA Gap Trends (Top 10 by emission):")
    print(sep)
    print(header)
    print(sep)
    for res in results:
        price_trend = "Converging" if res['delta_gap_price'] < 0 else "Diverging" if res['delta_gap_price'] > 0 else "Neutral"
        emission_trend = "Converging" if res['delta_gap_emission'] < 0 else "Diverging" if res['delta_gap_emission'] > 0 else "Neutral"
        price_color = RED if res['delta_gap_price'] < 0 else GREEN if res['delta_gap_price'] > 0 else ""
        emission_color = RED if res['delta_gap_emission'] < 0 else GREEN if res['delta_gap_emission'] > 0 else ""
        row = (
            f"{res['netuid']:>6} | "
            f"{res['final_ema5_price']:12.6f} | "
            f"{res['final_ema60_price']:12.6f} | "
            f"{res['final_gap_price']:10.6f} | "
            f"{res['delta_gap_price']:12.6f} | {price_color}{price_trend:10}{RESET} || "
            f"{res['final_ema5_emission']:14.6f} | "
            f"{res['final_ema60_emission']:14.6f} | "
            f"{res['final_gap_emission']:12.6f} | "
            f"{res['delta_gap_emission']:16.6f} | {emission_color}{emission_trend:10}{RESET}"
        )
        print(row)
    print(sep)

def main():
    # Hyperparameters for global processing.
    window_minutes = 240              # Global window for querying subnet-level records.
    max_gap_seconds = 75              # Maximum allowed gap between timestamps.
    alpha = 2 / (window_minutes + 1)
    max_tf = 240  # maximum timeframe in minutes for global calculations
    roll_window = 5
    savgol_window = 5
    polyorder = 2

    # Load .env variables if present.
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

    # Connect to the MySQL database.
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    # Get the latest snapshot timestamp.
    cursor.execute("SELECT MAX(snapshot_timestamp) AS max_ts FROM subnet_snapshots")
    max_ts_result = cursor.fetchone()
    if not max_ts_result or not max_ts_result['max_ts']:
        print("No snapshot data found in the database.")
        return
    max_ts_str = max_ts_result['max_ts'].strftime('%Y-%m-%d %H:%M:%S')
    print("Latest snapshot timestamp:", max_ts_str)
    
    max_ts_dt = datetime.strptime(max_ts_str, '%Y-%m-%d %H:%M:%S')
    lower_bound = (max_ts_dt - timedelta(minutes=window_minutes)).strftime('%Y-%m-%d %H:%M:%S')

    # Query records for subnet-level processing.
    query = """
    SELECT r.netuid, r.price, r.emission, s.snapshot_timestamp
    FROM subnet_records r
    JOIN subnet_snapshots s ON r.snapshot_id = s.snapshot_id
    WHERE s.snapshot_timestamp >= %s
    ORDER BY r.netuid, s.snapshot_timestamp
    """
    cursor.execute(query, (lower_bound,))
    records = cursor.fetchall()
    if not records:
        print("No records found in the past", window_minutes, "minutes.")
        return

    # Group records by netuid and ignore subnet 0.
    groups = defaultdict(list)
    for rec in records:
        if rec['netuid'] == 0:
            continue
        groups[rec['netuid']].append(rec)
    
    # --- Subnet-Level 5m vs 60m EMA Gap Trend Analysis ---
    subnet_results = []
    for netuid, recs in groups.items():
        # Ensure the records are sorted by timestamp.
        recs.sort(key=lambda r: r['snapshot_timestamp'])
        gap_info = compute_subnet_gap_trends(recs)
        if gap_info is None:
            continue
        # Also capture the current emission from the latest record for filtering.
        current_emission = float(recs[-1]['emission'])
        subnet_results.append({
            'netuid': netuid,
            'final_ema5_price': gap_info['final_ema5_price'],
            'final_ema60_price': gap_info['final_ema60_price'],
            'final_gap_price': gap_info['final_gap_price'],
            'delta_gap_price': gap_info['delta_gap_price'],
            'final_ema5_emission': gap_info['final_ema5_emission'],
            'final_ema60_emission': gap_info['final_ema60_emission'],
            'final_gap_emission': gap_info['final_gap_emission'],
            'delta_gap_emission': gap_info['delta_gap_emission'],
            'current_emission': current_emission
        })

    # Filter to top 10 subnets based on current emission (descending).
    subnet_results.sort(key=lambda x: x['current_emission'], reverse=True)
    top_subnets = subnet_results[:10]
    # Optionally, re-sort top subnets by netuid if needed.
    top_subnets.sort(key=lambda x: x['netuid'])

    display_subnet_ema_gap_table(top_subnets)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
