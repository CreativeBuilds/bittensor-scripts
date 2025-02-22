import os
import math
import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict
from scipy.signal import savgol_filter  # For smoothing the acceleration vector
import numpy as np

def compute_acceleration(time_series, nominal_timeframe):
    """
    Given a sorted list of (timestamp, total_price) and a nominal timeframe (in minutes),
    compute the EMA of total_price using the effective timeframe (i.e. actual data span if less than nominal).
    Then compute the acceleration as the rate of change of (TP - EMA) between the last two data points.
    Returns acceleration (per minute) or None if not computable.
    """
    if not time_series:
        return None
    # Determine the actual available time span in minutes
    actual_time = (time_series[-1][0] - time_series[0][0]).total_seconds() / 60.0
    # Use the full available span if it's less than the nominal timeframe
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

def main():
    # Hyperparameters
    window_minutes = 240              # EMA window (minutes) for subnet-level calculations.
    max_gap_seconds = 75              # Maximum allowed gap between timestamps.
    timeframes = [5, 10, 15, 60, 240]  # Nominal timeframes for acceleration analysis (in minutes).
    alpha = 2 / (window_minutes + 1)    # Smoothing factor for subnet-level EMA.
    max_tf = max(timeframes)           # Maximum nominal timeframe for historical query.
    roll_window = 5                   # Rolling window size for acceleration computation.
    savgol_window = 5                 # Window length for Savitzky-Golay filter (must be odd).
    polyorder = 2                     # Polynomial order for Savitzky-Golay filter.

    # ANSI escape sequences for colors.
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

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

    # Query records for subnet-level processing (last window_minutes minutes).
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

    # Group records by netuid (for subnet-level calculations).
    from collections import defaultdict
    groups = defaultdict(list)
    for rec in records:
        groups[rec['netuid']].append(rec)
    
    # Compute the current total price (for subnet-level, excluding netuid 0).
    total_price = sum(float(recs[-1]['price']) for netuid, recs in groups.items() if netuid != 0)

    # --- Total Price EMA and Acceleration Calculation ---
    # Query records for the historical time series (last max_tf minutes).
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
    # Build total_by_ts: for each timestamp, sum prices from netuid != 0.
    total_by_ts = {}
    for rec in records_tf:
        if rec['netuid'] == 0:
            continue
        ts = rec['snapshot_timestamp']
        total_by_ts.setdefault(ts, 0)
        total_by_ts[ts] += float(rec['price'])
    # Create a sorted time series: list of (timestamp, total_price).
    time_series = sorted(total_by_ts.items())

    # Compute Total Price EMA over the entire time series using the same alpha.
    ema_total = None
    for ts, tp in time_series:
        if ema_total is None:
            ema_total = tp
        else:
            ema_total = alpha * tp + (1 - alpha) * ema_total

    # Display Total Price and Total Price EMA.
    print(f"Total Price: {total_price:.6f}")
    if total_price > ema_total:
        total_trend_str = f"{GREEN}{ema_total:.6f}{RESET}"
    else:
        total_trend_str = f"{RED}{total_price:.6f}{RESET}"
    print(f"Total Price EMA: {total_trend_str}")

    # Compute acceleration metrics for each nominal timeframe.
    print("\nAcceleration/Deceleration of Total Price (TP - EMA) per timeframe:")
    if time_series:
        available_tf = (time_series[-1][0] - time_series[0][0]).total_seconds() / 60.0
    else:
        available_tf = 0

    for tf in timeframes:
        # If we don't have enough data to cover this timeframe, print N/A
        if available_tf < tf:
            acc_str = "N/A"
        else:
            # Filter to data within the last 'tf' minutes
            lower_bound_current = max_ts_dt - timedelta(minutes=tf)
            ts_filtered = [(ts, tp) for ts, tp in time_series if ts >= lower_bound_current]
            # Compute rolling acceleration vector
            roll_accels = compute_rolling_acceleration(ts_filtered, tf, roll_window)
            if len(roll_accels) < 3:
                smoothed_accel = roll_accels[-1] if roll_accels else None
            else:
                # Use Savitzky-Golay filter to smooth the acceleration vector.
                smoothed_accel = savgol_filter(np.array(roll_accels), window_length=savgol_window, polyorder=polyorder)[-1]
            if smoothed_accel is None:
                acc_str = "N/A"
            else:
                if smoothed_accel > 0:
                    acc_str = f"{GREEN}{smoothed_accel:+.6f}{RESET}"
                else:
                    acc_str = f"{RED}{smoothed_accel:+.6f}{RESET}"
        print(f"  {tf}m: {acc_str}")

    # For subnet-level processing: compute subnet EMAs and other stats.
    max_candle_count = 0
    results = []
    for netuid, recs in groups.items():
        ema_price = None
        ema_emission = None
        prices = []  # For standard deviation calculation.
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
                if diff > max_gap_seconds:
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

    print(f"\nEMA - {window_minutes}m ({max_candle_count} objects)")

    # Sort subnet results by current emission descending and take the top 10.
    results = sorted(results, key=lambda x: x['current_emission'], reverse=True)[:10]

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
