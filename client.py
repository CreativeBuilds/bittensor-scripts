import requests
import time

BASE_URL = "http://localhost:8000"

def test_latest_snapshot():
    url = f"{BASE_URL}/snapshots/latest"
    try:
        response = requests.get(url)
        print("\n--- Testing /snapshots/latest ---")
        print(f"Status Code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print(f"Error testing /snapshots/latest: {e}")

def test_snapshot_by_id(snapshot_id):
    url = f"{BASE_URL}/snapshots/{snapshot_id}"
    try:
        response = requests.get(url)
        print(f"\n--- Testing /snapshots/{snapshot_id} ---")
        print(f"Status Code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print(f"Error testing /snapshots/{snapshot_id}: {e}")

def test_subnets(filters=None):
    url = f"{BASE_URL}/subnets"
    try:
        params = filters if filters is not None else {}
        response = requests.get(url, params=params)
        print("\n--- Testing /subnets ---")
        if filters:
            print(f"Filters: {filters}")
        print(f"Status Code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print(f"Error testing /subnets: {e}")

def test_rate_limit():
    url = f"{BASE_URL}/subnets"
    print("\n--- Testing Rate Limiting Cache ---")
    try:
        # First request - should fetch fresh data
        response1 = requests.get(url)
        data1 = response1.json()
        print("First response (fresh):")
        print(data1)
        
        # Wait less than 10 seconds
        time.sleep(5)
        
        # Second request - should be served from cache
        response2 = requests.get(url)
        data2 = response2.json()
        print("Second response (cached):")
        print(data2)
        
        # Optionally, check if the cached responses are identical
        if data1 == data2:
            print("Cache working as expected: responses are identical.")
        else:
            print("Cache may not be working as expected: responses differ.")
    except Exception as e:
        print(f"Error testing rate limit: {e}")

if __name__ == "__main__":
    # Run tests one after the other
    test_latest_snapshot()
    # Test a specific snapshot by id (change the id based on your data)
    test_snapshot_by_id(1)
    # Test retrieving all subnets
    test_subnets()
    # Test filtering on subnets (example: filter by netuid=1)
    test_subnets(filters={"netuid": 1})
    # Test the caching behavior for rate limiting
    test_rate_limit()
