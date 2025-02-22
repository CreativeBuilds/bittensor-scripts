import time
import csv
import os
import bittensor
import mysql.connector

# --- Database Configuration ---
# These environment variables can be set in your shell or via a config management tool
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST') or os.environ.get('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER') or os.environ.get('DB_USER', 'liquidity'),
    'password': os.getenv('DB_PASSWORD') or os.environ.get('DB_PASSWORD', 'hackermanimin'),
    'database': os.getenv('DB_NAME') or os.environ.get('DB_NAME', 'prices')
}

# Connect to the MySQL database running on the same EC2 instance
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS subnet_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp VARCHAR(255) NOT NULL,
        subnet_data TEXT NOT NULL
    )
''')
conn.commit()

# Connect to the Bittensor network (e.g., testnet or mainnet)
sub = bittensor.Subtensor()

while True:
    # Fetch all subnet info (DynamicInfo objects)
    subnet_infos = sub.all_subnets()  # Requires network access to the chain

    subnet_data = []
    for info in subnet_infos:
        # Get reserves (assumed to be in RAO for TAO reserves)
        tau_in_rao   = info.tao_in.rao      # TAO reserve in raw RAO units
        alpha_in_rao = info.alpha_in.rao    # Alpha reserve in raw smallest units
        
        # Compute current price as TAO/alpha
        current_price = tau_in_rao / alpha_in_rao
        
        # --- Emission Calculation (following btcli logic) ---
        if info.netuid == 0:
            # For the root subnet (netuid 0) emission is set to 0
            emission_val = 0.0
        else:
            try:
                # Use the tao_in_emission property to obtain the emission in TAO
                emission_val = info.tao_in_emission.tao
            except Exception as e:
                print(f"DEBUG: Error retrieving emission for netuid {info.netuid}: {e}")
                emission_val = 0.0
        # -----------------------------------------------------

        subnet_data.append({
            'netuid': info.netuid,
            'subnet_name': info.subnet_name,
            'price': current_price,
            'emission': emission_val,
            'symbol': info.symbol
        })

    # Sort by emission descending
    subnet_data_sorted = sorted(subnet_data, key=lambda x: x['emission'], reverse=True)

    # Separate top 10 (for console) from the rest (to log file)
    top10 = subnet_data_sorted[:10]
    rest = subnet_data_sorted[10:]

    # Get current timestamp
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # --- Console Logging (Top 10 by Emission) ---
    print(f"\nTimestamp: {timestamp}")
    print("Top 10 Subnets by Emission:")
    for data in top10:
        print(f"Netuid: {data['netuid']}  Subnet: {data['subnet_name']}  Price: {data['price']:.4f} {data['symbol']}  Emission: {data['emission']:.4f}")
    
    # --- File Logging (Rest of the Subnets) ---
    if rest:
        with open("subnet_rest.log", "a") as log_file:
            log_file.write(f"\nTimestamp: {timestamp}\n")
            for data in rest:
                log_file.write(f"Netuid: {data['netuid']}  Subnet: {data['subnet_name']}  Price: {data['price']:.4f} {data['symbol']}  Emission: {data['emission']:.4f}\n")
    
    # --- MySQL Logging ---
    # Serialize subnet_data_sorted: each record as netuid|price|emission, separated by semicolons.
    csv_data_str = ";".join(f"{data['netuid']}|{data['price']:.4f}|{data['emission']:.4f}" for data in subnet_data_sorted)
    insert_query = "INSERT INTO subnet_logs (timestamp, subnet_data) VALUES (%s, %s)"
    cursor.execute(insert_query, (timestamp, csv_data_str))
    conn.commit()
    
    # --- CSV Logging (Optional) ---
    csv_file_exists = os.path.isfile("subnets.csv")
    with open("subnets.csv", "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not csv_file_exists:
            writer.writerow(["timestamp", "subnet_data"])  # Write header if new file
        writer.writerow([timestamp, csv_data_str])
    
    # Wait 60 seconds before the next check
    time.sleep(60)
