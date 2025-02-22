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

def compute_acceleration(time_series, nominal_timeframe):
    """
    Given a sorted list of (timestamp, total_price) and a nominal timeframe (in minutes),
    compute the EMA of total_price using the effective timeframe (i.e. actual data span if less than nominal).
    Then compute the acceleration as the rate of change of (TP - EMA) between the last two data points.
    Returns acceleration (per minute) or None if not computable.
    """
    if not time_series:
        return None
    actual_time = (time_series[-1][0] - time_series[0][0]).total_seconds() / 60.0
    effective_tf = nominal_timeframe if actual_time >= nominal_timeframe else actual_time
    if effective_tf <= 0:
        return None

    alpha_tf = 2 / (effective_tf + 1)
    ema = None
    diffs = []  # List of (timestamp, TP - EMA)
    for ts, tp in time_series:
        if ema is None:
            ema = tp
        else:
            ema = alpha_tf * tp + (1 - alpha_tf) * ema
        diff = tp - ema
        diffs.append((ts, diff))
    if len(diffs) < 2:
        return None
    ts_prev, diff_prev = diffs[-2]
    ts_last, diff_last = diffs[-1]
    dt = (ts_last - ts_prev).total_seconds() / 60.0
    if dt == 0:
        return None
    acceleration = (diff_last - diff_prev) / dt
    return acceleration

def compute_rolling_acceleration(time_series, nominal_timeframe, roll_window=5):
    """
    Computes a rolling acceleration vector for a given time_series and nominal timeframe.
    For each rolling window (of size roll_window), compute acceleration.
    Returns a list of acceleration values.
    """
    roll_accels = []
    n = len(time_series)
    if n < roll_window:
        return roll_accels
    for i in range(n - roll_window + 1):
        window = time_series[i:i+roll_window]
        accel = compute_acceleration(window, nominal_timeframe)
        if accel is not None:
            roll_accels.append(accel)
    return roll_accels

def compute_ema(time_series, timeframe):
    """
    Compute the EMA for a given time_series (list of (timestamp, value)) over the specified timeframe.
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

def compute_derivative(series):
    """
    Given a time series (list of (timestamp, value)), compute the derivative (rate of change)
    between consecutive points.
    Returns a list of (timestamp, derivative_value).
    """
    derivative = []
    for i in range(1, len(series)):
        ts_prev, value_prev = series[i-1]
        ts_curr, value_curr = series[i]
        dt = (ts_curr - ts_prev).total_seconds() / 60.0  # minutes
        if dt > 0:
            deriv = (value_curr - value_prev) / dt
            derivative.append((ts_curr, deriv))
    return derivative

def average_gap_change(gap_series, current_time, window=timedelta(minutes=5)):
    """
    Calculate the average change in the gap over the last 'window' duration.
    
    gap_series: list of (timestamp, gap_value) sorted by timestamp.
    current_time: the most recent timestamp (datetime object).
    window: duration over which to calculate the average change (default 5 minutes).
    
    Returns the average rate of change per minute.
    """
    start_time = current_time - window
    relevant_points = [(ts, gap) for ts, gap in gap_series if start_time <= ts <= current_time]
    if len(relevant_points) < 2:
        return None  # Not enough data to compute a rate
    start_ts, start_gap = relevant_points[0]
    end_ts, end_gap = relevant_points[-1]
    dt = (end_ts - start_ts).total_seconds() / 60.0
    if dt == 0:
        return None
    avg_change = (end_gap - start_gap) / dt
    return avg_change

def compute_subnet_emas(records):
    """
    For a given subnet's records (assumed sorted by timestamp),
    compute the 5-minute EMA and 60-minute EMA for both price and emission.
    We filter the records based on the subnet's latest timestamp.
    Returns:
      (ema_5_price, ema_60_price, ema_5_emission, ema_60_emission)
    or None if insufficient data.
    """
    last_ts = records[-1]['snapshot_timestamp']
    # For 5-minute EMA, use records from last 5 minutes.
    window_5 = last_ts - timedelta(minutes=5)
    series_5_price = [(rec['snapshot_timestamp'], float(rec['price'])) for rec in records if rec['snapshot_timestamp'] >= window_5]
    series_5_emission = [(rec['snapshot_timestamp'], float(rec['emission'])) for rec in records if rec['snapshot_timestamp'] >= window_5]
    # For 60-minute EMA, use records from last 60 minutes.
    window_60 = last_ts - timedelta(minutes=60)
    series_60_price = [(rec['snapshot_timestamp'], float(rec['price'])) for rec in records if rec['snapshot_timestamp'] >= window_60]
    series_60_emission = [(rec['snapshot_timestamp'], float(rec['emission'])) for rec in records if rec['snapshot_timestamp'] >= window_60]
    if not series_5_price or not series_60_price or not series_5_emission or not series_60_emission:
        return None
    ema_5_price = compute_ema(series_5_price, 5)[-1][1]
    ema_60_price = compute_ema(series_60_price, 60)[-1][1]
    ema_5_emission = compute_ema(series_5_emission, 5)[-1][1]
    ema_60_emission = compute_ema(series_60_emission, 60)[-1][1]
    return ema_5_price, ema_60_price, ema_5_emission, ema_60_emission

def display_subnet_ema_comparison_table(results):
    """
    Display a table comparing the 5m and 60m EMAs for price and emission for each subnet.
    """
    header = (
        f"{'netuid':>6} | {'EMA_5 Price':>12} | {'EMA_60 Price':>12} | {'Price Diff':>12} | {'Price Trend':>12} || "
        f"{'EMA_5 Emission':>14} | {'EMA_60 Emission':>14} | {'Emission Diff':>14} | {'Emission Trend':>14}"
    )
    sep = "-" * len(header)
    print("\nSubnet 5m vs 60m EMA Comparison:")
    print(sep)
    print(header)
    print(sep)
    for res in results:
        price_color = GREEN if res['diff_price'] > 0 else RED if res['diff_price'] < 0 else ""
        emission_color = GREEN if res['diff_emission'] > 0 else RED if res['diff_emission'] < 0 else ""
        row = (
            f"{res['netuid']:>6} | "
            f"{res['ema5_price']:12.6f} | "
            f"{res['ema60_price']:12.6f} | "
            f"{res['diff_price']:12.6f} | {price_color}{res['price_trend']:12}{RESET} || "
            f"{res['ema5_emission']:14.6f} | "
            f"{res['ema60_emission']:14.6f} | "
            f"{res['diff_emission']:14.6f} | {emission_color}{res['emission_trend']:14}{RESET}"
        )
        print(row)
    print(sep)

def main():
    # Hyperparameters for overall processing.
    window_minutes = 240              # Global window for querying subnet-level records.
    max_gap_seconds = 75              # Maximum allowed gap between timestamps.
    timeframes = [5, 10, 15, 60, 240]  # Nominal timeframes for acceleration analysis (in minutes).
    alpha = 2 / (window_minutes + 1)    # Smoothing factor for global EMA.
    max_tf = max(timeframes)
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
    
    # Compute lower bound for subnet-level query.
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

    # Group records by netuid.
    groups = defaultdict(list)
    for rec in records:
        groups[rec['netuid']].append(rec)
    
    # (Optional) Global Total Price EMA and other metrics can remain here...
    total_price = sum(float(recs[-1]['price']) for netuid, recs in groups.items() if netuid != 0)
    # Query for global historical time series...
    lower_bound_tf = (max_ts_dt - timedelta(minutes=max_tf)).strftime('%Y-%m-%d %H:%M:%S')
    query_tf = """
    SELECT r.netuid, r.price, s.snapshot_timestamp
    FROM subnet_records r
    JOIN subnet_snapshots s ON r.snapshot_id = s.snapshot_id
    WHERE s.snapshot_timestamp >= %s
    ORDER BY s.snapshot_timestamp
    """
    cursor.execute(query_tf, (lower_bound_tf,))
    records_tf = cursor.fetchall()
    total_by_ts = {}
    for rec in records_tf:
        if rec['netuid'] == 0:
            continue
        ts = rec['snapshot_timestamp']
        total_by_ts.setdefault(ts, 0)
        total_by_ts[ts] += float(rec['price'])
    time_series = sorted(total_by_ts.items())
    ema_total = None
    for ts, tp in time_series:
        if ema_total is None:
            ema_total = tp
        else:
            ema_total = alpha * tp + (1 - alpha) * ema_total
    print(f"Total Price: {total_price:.6f}")
    total_trend_str = f"{GREEN}{ema_total:.6f}{RESET}" if total_price > ema_total else f"{RED}{total_price:.6f}{RESET}"
    print(f"Total Price EMA: {total_trend_str}")

    # --- (Optional) Global EMA Gap Analysis and Acceleration Metrics ---
    ema_5m_global = compute_ema(time_series, 5)
    ema_60m_global = compute_ema(time_series, 60)
    ema_gap = [(ts, e5 - e60) for ((ts, e5), (_, e60)) in zip(ema_5m_global, ema_60m_global)]
    gap_derivative = compute_derivative(ema_gap)
    
    print("\n5m vs 60m EMA Gap Analysis (Global):")
    for ts, deriv in gap_derivative:
        if deriv < 0:
            print(f"At {ts}, 5m EMA is converging towards 60m EMA (gap decreasing at {deriv:+.6f}/min).")
        else:
            print(f"At {ts}, 5m EMA is diverging from 60m EMA (gap increasing at {deriv:+.6f}/min).")
    if ema_gap:
        current_time = ema_gap[-1][0]
        avg_derivative = average_gap_change(ema_gap, current_time)
        if avg_derivative is not None:
            if avg_derivative < 0:
                print(f"\nOver the last 5 minutes, global gap has been shrinking at {avg_derivative:+.6f}/min.")
            else:
                print(f"\nOver the last 5 minutes, global gap has been widening at {avg_derivative:+.6f}/min.")
        else:
            print("\nNot enough data for global average gap change.")
    else:
        print("\nNo global EMA gap data available.")

    # --- Subnet-Level 5m vs 60m EMA Comparison ---
    subnet_results = []
    for netuid, recs in groups.items():
        # Ensure records for this subnet are sorted by timestamp.
        recs.sort(key=lambda r: r['snapshot_timestamp'])
        emas = compute_subnet_emas(recs)
        if emas is None:
            continue
        ema5_price, ema60_price, ema5_emission, ema60_emission = emas
        diff_price = ema5_price - ema60_price
        diff_emission = ema5_emission - ema60_emission
        price_trend = "Diverging" if diff_price > 0 else "Converging" if diff_price < 0 else "Neutral"
        emission_trend = "Diverging" if diff_emission > 0 else "Converging" if diff_emission < 0 else "Neutral"
        subnet_results.append({
            'netuid': netuid,
            'ema5_price': ema5_price,
            'ema60_price': ema60_price,
            'diff_price': diff_price,
            'price_trend': price_trend,
            'ema5_emission': ema5_emission,
            'ema60_emission': ema60_emission,
            'diff_emission': diff_emission,
            'emission_trend': emission_trend
        })

    # Sort subnet results by netuid (or any other metric as desired)
    subnet_results.sort(key=lambda x: x['netuid'])
    display_subnet_ema_comparison_table(subnet_results)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
