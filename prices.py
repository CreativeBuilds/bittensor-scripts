import os
import math
import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict

def main():
    # Hyperparameter: number of minutes for the EMA window.
    window_minutes = 12
    alpha = 2 / (window_minutes + 1)  # Smoothing factor for the EMA

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
    
    # Compute the lower bound for the query window
    max_ts = datetime.strptime(max_ts_str, '%Y-%m-%d %H:%M:%S')
    lower_bound = (max_ts - timedelta(minutes=window_minutes)).strftime('%Y-%m-%d %H:%M:%S')

    # Query records from the last window_minutes minutes
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

    # Group records by netuid
    groups = defaultdict(list)
    for rec in records:
        groups[rec['netuid']].append(rec)
    
    # Compute the current total price across all subnets excluding netuid 0.
    total_price = sum(float(recs[-1]['price']) for netuid, recs in groups.items() if netuid != 0)

    # Compute the Total Price EMA by grouping records by snapshot timestamp (excluding netuid 0)
    total_by_ts = {}
    for rec in records:
        if rec['netuid'] == 0:
            continue
        ts = rec['snapshot_timestamp']
        total_by_ts.setdefault(ts, 0)
        total_by_ts[ts] += float(rec['price'])
    # Sort timestamps in increasing order and compute EMA for total price
    sorted_ts = sorted(total_by_ts.keys())
    ema_total = None
    for ts in sorted_ts:
        current_total = total_by_ts[ts]
        if ema_total is None:
            ema_total = current_total
        else:
            ema_total = alpha * current_total + (1 - alpha) * ema_total

    # Display the Total Price and Total Price EMA
    print(f"Total Price: {total_price:.6f}")
    if total_price > ema_total:
        total_ema_str = f"{GREEN}{ema_total:.6f}{RESET}"
    else:
        total_ema_str = f"{RED}{ema_total:.6f}{RESET}"
    print(f"Total Price EMA: {total_ema_str}")

    max_candle_count = 0  # Will track the maximum number of candles (timestamps) across subnets
    results = []

    # Process each group: compute EMA for price and emission, standard deviation, and validate time gaps
    for netuid, recs in groups.items():
        ema_price = None
        ema_emission = None
        prices = []  # For standard deviation calculation
        candle_count = 0

        # Validate that each record is within 65 seconds of the previous one
        for i, rec in enumerate(recs):
            price = float(rec['price'])
            emission_val = float(rec['emission'])
            prices.append(price)
            candle_count += 1

            if i > 0:
                prev_time = recs[i-1]['snapshot_timestamp']
                curr_time = rec['snapshot_timestamp']
                diff = (curr_time - prev_time).total_seconds()
                if diff > 75:
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

    # Now print the EMA window info along with the max candle count
    print(f"EMA - {window_minutes}m ({max_candle_count} objects)")

    # Sort results by current emission descending and take the top 10
    results = sorted(results, key=lambda x: x['current_emission'], reverse=True)[:10]

    # Print the table header with separate columns for current and EMA values
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
