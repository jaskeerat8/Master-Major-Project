import requests

url = "http://localhost:8000"
response = requests.get(url)
print(response.json())

url = "http://localhost:8000/get_info"
block_list = [835899]
response = requests.get(url, params={"block_list": block_list})
print(response.json())

url = "http://localhost:8000/get_info"
transaction_list = ['e24040cd57c45e8ffcf2771b5b7b6fc28326d5ad5728cc70d8a887c7a2d74ce3', 'ff634eaafce0e53780455e849115124327ea441c17d338318f8ab3d77be9c029']
response = requests.get(url, params={"transaction_list": transaction_list})
print(response.json())

url = "http://localhost:8000/get_data"
block_list = [835899]
transaction_list = ['e24040cd57c45e8ffcf2771b5b7b6fc28326d5ad5728cc70d8a887c7a2d74ce3', 'ff634eaafce0e53780455e849115124327ea441c17d338318f8ab3d77be9c029']
response = requests.get(url, params={"block_list": block_list, "transaction_list": transaction_list})
print(response.json())

url = "http://localhost:8000/get_transaction"
response = requests.get(url, params={"transaction_id": "60e4979669266408de3aad95d388cf1594cfa9d7142f8b64a25d8a1cabc3a302"})
print(response.json())
