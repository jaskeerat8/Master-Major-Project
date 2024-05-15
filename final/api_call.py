import requests

url = "http://localhost:8000"
response = requests.get(url)
print(response.json(), "\n")

url = "http://localhost:8000/get_info"
block_number = 842523
response = requests.get(url, params={"block_number": block_number})
print("Returned Block Data:")
print(response.json(), "\n")

url = "http://localhost:8000/get_info"
transaction = "96b0d1c891ec2abbec4bcf472bd2a6cb37a2e10ff0848098f05530166c833288"
response = requests.get(url, params={"transaction_id": transaction})
print("Returned Transaction Data:")
print(response.json(), "\n")

url = "http://localhost:8000/get_alert_data"
response = requests.get(url)
print("Returned list of Transaction ID with anomalies:")
print(response.json(), "\n")

url = "http://localhost:8000/get_alert_data"
block_number = 842523
response = requests.get(url, params={"block_number": block_number})
print(f"Returned list of Transaction ID with anomalies in block {block_number}:")
print(response.json(), "\n")

url = "http://localhost:8000/get_transaction"
transaction_id = "ac98f382d472623a836c36b5f0a1f53e61255aabf6c5d4d929a3bda13a88dc26"
response = requests.get(url, params={"transaction_id": transaction_id})
print("Returned Processed Transaction Data:")
print(response.json(), "\n")


