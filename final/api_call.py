import requests

# url = "http://localhost:8000"
# response = requests.get(url)
# print(response.json())

# url = "http://localhost:8000/get_info"
# block_list = [835899]
# response = requests.get(url, params={"block_list": block_list})
# print("Returned Block Data:")
# print(response.json(), "\n")

# url = "http://localhost:8000/get_info"
# transaction_list = ['e24040cd57c45e8ffcf2771b5b7b6fc28326d5ad5728cc70d8a887c7a2d74ce3', 'ff634eaafce0e53780455e849115124327ea441c17d338318f8ab3d77be9c029']
# response = requests.get(url, params={"transaction_list": transaction_list})
# print("Returned Transaction Data:")
# print(response.json(), "\n")

# url = "http://localhost:8000/get_data"
# block_list = [835899]
# transaction_list = ['e24040cd57c45e8ffcf2771b5b7b6fc28326d5ad5728cc70d8a887c7a2d74ce3', 'ff634eaafce0e53780455e849115124327ea441c17d338318f8ab3d77be9c029']
# response = requests.get(url, params={"block_list": block_list, "transaction_list": transaction_list})
# print("Returned Transaction Info:")
# print(response.json(), "\n")

# url = "http://192.168.19.148/query_data"
# transaction_id = "63064d658bcae6c5b954101a8ee57d597a433804a83e97655f1946784fd0649b"
# response = requests.get(url, params={"transaction_id": transaction_id})
# print("Returned Transaction Data:")
# print(response.json(), "\n")

url = "http://localhost:8000/get_transaction"
transaction_id = "aed20686fca3f354b32ba0b9bcc93724fac46430062d6a353824469faa301dd9"
response = requests.get(url, params={"transaction_id": transaction_id})
print("Returned Transaction Data:")
print(response.json(), "\n")

# url = "https://doe-good-formally.ngrok-free.app/get_alert_data"
# response = requests.get(url)
# print("Returned list of Transaction ID:")
# print(response.json(), "\n")
