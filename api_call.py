import requests

# url = "http://localhost:8000/"
# response = requests.get(url)
# print(response.json())

# url = "http://localhost:8000/get_info"
# block_list = [825904]
# transaction_list = ["9b018eb20e7dbd3c3ce683cdc049a7e820a4283598d4d254ede90e53d2643a5b", "20c4c2f250ac30d1ad14c6e4a0e53752706b8319915cc9c8be8127bbaaac4be2"]
# response = requests.get(url, params={"block_list": block_list})
# print(response.json())

url = "http://localhost:8000/get_data"
block_list = [825904]
transaction_list = ["9b018eb20e7dbd3c3ce683cdc049a7e820a4283598d4d254ede90e53d2643a5b", "20c4c2f250ac30d1ad14c6e4a0e53752706b8319915cc9c8be8127bbaaac4be2"]
#start_timestamp =
#end_timestamp =
response = requests.get(url, params={"block_list": block_list, "transaction_list": transaction_list})
print(response.json())

# url = "http://localhost:8000/post_alert"
# transaction_list = ["9b018eb20e7dbd3c3ce683cdc049a7e820a4283598d4d254ede90e53d2643a5b", "20c4c2f250ac30d1ad14c6e4a0e53752706b8319915cc9c8be8127bbaaac4be2"]
# response = requests.post(url, params={"transaction_list": transaction_list})
# print(response.json())
