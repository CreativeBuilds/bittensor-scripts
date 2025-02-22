import os
import math
import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict

def compute_acceleration(time_series, timeframe):
    """
    Given a sorted list of (timestamp, total_price) and a timeframe (in minutes),
    compute the EMA of total_price (using alpha = 2/(timeframe+1)) and then compute
    the acceleration as the rate of change of (TP - EMA) between the last two data points.
    Returns acceleration (per minute) or None if not computable.
    """
    alpha_tf = 2 / (timeframe + 1)
    ema = None
    diffs = []  # List of tuples (timestamp, TP - EMA)
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

def main():
    # Hyperparameter for subnet processing: number of minutes for the EMA window.
    window_minutes = 12
    alpha = 2 / (window_minutes + 1)  # Smoothing factor for the EMA (for subnet-level)

    # Timeframes for acceleration analysis (in minutes)
    timeframes = [5, 10, 15, 60, 240]
    max_tf = max(timeframes)  # Maximum timeframe for historical query

    # ANSI escape sequences for colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    # Load .env variables if present
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

    # Connect to the MySQL database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    # Get the latest snapshot timestamp
    cursor.execute("SELECT MAX(snapshot_timestamp) AS max_ts FROM subnet_snapshots")
    max_ts_result = cursor.fetchone()
    if not max_ts_result or not max_ts_result['max_ts']:
        print("No snapshot data found in the database.")
        return

    max_ts_str = max_ts_result['max_ts'].strftime('%Y-%m-%d %H:%M:%S')
    print("Latest snapshot timestamp:", max_ts_str)
    
    # Compute lower bound for subnet-level query (window_minutes)
    max_ts_dt = datetime.strptime(max_ts_str, '%Y-%m-%d %H:%M:%S')
    lower_bound = (max_ts_dt - timedelta(minutes=window_minutes)).strftime('%Y-%m-%d %H:%M:%S')

    # Query records for subnet-level processing (last window_minutes minutes)
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

    # Group records by netuid (for subnet-level calculations)
    groups = defaultdict(list)
    for rec in records:
        groups[rec['netuid']].append(rec)
    
    # Compute the current total price (for subnet-level, excluding netuid 0)
    total_price = sum(float(recs[-1]['price']) for netuid, recs in groups.items() if netuid != 0)

    # --- Total Price EMA and Acceleration Calculation ---
    # Query records for the historical time series (last max_tf minutes)
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
    # Build total_by_ts dictionary: for each timestamp, sum prices from netuid != 0
    total_by_ts = {}
    for rec in records_tf:
        if rec['netuid'] == 0:
            continue
        ts = rec['snapshot_timestamp']
        total_by_ts.setdefault(ts, 0)
        total_by_ts[ts] += float(rec['price'])
    # Create a sorted time series: list of (timestamp, total_price)
    time_series = sorted(total_by_ts.items())

    # Compute Total Price EMA using the full time series and same smoothing factor as before (for reporting, though we'll compare trends)
    # Here we compute an EMA over the entire time series using alpha for window_minutes? 
    # However, for acceleration analysis we recompute per timeframe below.
    ema_total = None
    for ts, tp in time_series:
        if ema_total is None:
            ema_total = tp
        else:
            ema_total = alpha * tp + (1 - alpha) * ema_total

    # Display Total Price and Total Price EMA
    print(f"Total Price: {total_price:.6f}")
    # Use the current total price if it is lower than its EMA (indicating downtrend)
    if total_price > ema_total:
        total_trend_str = f"{GREEN}{ema_total:.6f}{RESET}"
    else:
        total_trend_str = f"{RED}{total_price:.6f}{RESET}"
    print(f"Total Price EMA: {total_trend_str}")

    # Compute acceleration metrics for each timeframe and display
    print("\nAcceleration/Deceleration of Total Price (TP - EMA) per timeframe:")
    for tf in timeframes:
        lower_bound_current = max_ts_dt - timedelta(minutes=tf)
        # Filter the time_series for timestamps >= lower_bound_current
        ts_filtered = [(ts, tp) for ts, tp in time_series if ts >= lower_bound_current]
        if len(ts_filtered) < 2:
            acc_str = "N/A"
        else:
            acc = compute_acceleration(ts_filtered, tf)
            if acc is None:
                acc_str = "N/A"
            else:
                # Color green if acceleration is positive, red if negative
                if acc > 0:
                    acc_str = f"{GREEN}{acc:+.6f}{RESET}"
                else:
                    acc_str = f"{RED}{acc:+.6f}{RESET}"
        print(f"  {tf}m: {acc_str}")

    # For the subnet-level processing, update max_candle_count and compute subnet EMAs as before.
    max_candle_count = 0
    results = []
    for netuid, recs in groups.items():
        ema_price = None
        ema_emission = None
        prices = []  # For standard deviation calculation
        candle_count = 0
        for i, rec in enumerate(recs):
            price = float(rec['price'])
            emission_val = float(rec['emission'])
            prices.append(price)
            candle_count += 1
            if i > 0:
                prev_time = recs[i-1]['snapshot_timestamp']
                curr_time = rec['snapshot_timestamp']
                diff = (curr_time - prev_time).total_seconds()
                if diff > 65:
                    print(f"Warning: netuid {netuid} has a gap of {diff:.0f} seconds between candle {i} and candle {i+1}")
            if ema_price is None:
                ema_price = price
            else:
                ema_price = alpha * price + (1 - alpha) * ema_price
            if ema_emission is None:
                ema_emission = emission_val
            else:
                ema_emission = alpha * emission_val + (1 - alpha) * ema_emission
        if candle_count > max_candle_count:
            max_candle_count = candle_count
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        current = recs[-1]
        results.append({
            'netuid': netuid,
            'current_price': float(current['price']),
            'current_emission': float(current['emission']),
            'ema_price': ema_price,
            'ema_emission': ema_emission,
            'std_price': std_price
        })

    # Now print the EMA window info (for subnet-level) along with the max candle count
    print(f"\nEMA - {window_minutes}m ({max_candle_count} objects)")

    # Sort subnet results by current emission descending and take the top 10
    results = sorted(results, key=lambda x: x['current_emission'], reverse=True)[:10]

    # Print the table header for subnet details
    header = (f"{'netuid':>6}  {'Curr Price':>14}  {'EMA Price':>14}  "
              f"{'Curr Emission':>16}  {'EMA Emission':>14}  {'std_price':>12}")
    print(header)
    for res in results:
        if res['current_price'] > res['ema_price']:
            curr_price_str = f"{GREEN}{res['current_price']:14.6f}{RESET}"
        else:
            curr_price_str = f"{RED}{res['current_price']:14.6f}{RESET}"
        if res['current_emission'] > res['ema_emission']:
            curr_emission_str = f"{GREEN}{res['current_emission']:16.6f}{RESET}"
        else:
            curr_emission_str = f"{RED}{res['current_emission']:16.6f}{RESET}"
        print(f"{res['netuid']:>6}  {curr_price_str}  {res['ema_price']:14.6f}  "
              f"{curr_emission_str}  {res['ema_emission']:14.6f}  {res['std_price']:12.6f}")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
