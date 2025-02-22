import subprocess

def get_dtao_pricing(netuid):
    """
    Fetch the pricing (τ_in/α_in) for a given subnet netuid from btcli subnets list.
    
    Args:
        netuid (int): The netuid of the subnet to fetch pricing for.
    
    Returns:
        float: The price in τ, or None if not found or on error.
    """
    try:
        # Run btcli subnets list with the network flag
        output = subprocess.check_output(
            ["btcli", "subnets", "list", "--subtensor.network", "finney"],
            text=True
        )
        lines = output.splitlines()

        # Find the header line containing "Netuid" and "(τ_in/α_in)"
        header_line = None
        for i, line in enumerate(lines):
            if "Netuid" in line and "(τ_in/α_in)" in line:
                header_line = line
                separator_index = i + 1  # The next line should be the separator
                break
        if not header_line:
            raise ValueError("Header line not found in output")

        # Parse headers using ┃ as the separator
        headers = [h.strip() for h in header_line.split("┃") if h.strip()]
        
        # Find column indices
        netuid_index = headers.index("Netuid")
        price_index = headers.index("(τ_in/α_in)")

        # Parse data lines after the separator
        for line in lines[separator_index + 1:]:
            # Check if the line is a data row (starts with │)
            if line.strip().startswith("│"):
                # Split data line using │ as the separator
                columns = [c.strip() for c in line.split("│") if c.strip()]
                # Ensure there are enough columns
                if len(columns) > max(netuid_index, price_index):
                    current_netuid = columns[netuid_index]
                    # Match the requested netuid
                    if current_netuid == str(netuid):
                        # Extract the price (e.g., "0.0301 τ/γ" -> "0.0301")
                        price_str = columns[price_index].split()[0]
                        return float(price_str)
        print(f"Netuid {netuid} not found in the output")
        return None

    except subprocess.CalledProcessError as e:
        print(f"Error running btcli subnets list: {e}")
        return None
    except ValueError as e:
        print(f"Parsing error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Example usage
subnet_netuid = 3
price = get_dtao_pricing(subnet_netuid)
if price is not None:
    print(f"Price for netuid {subnet_netuid}: {price} τ")
else:
    print("Failed to fetch price")