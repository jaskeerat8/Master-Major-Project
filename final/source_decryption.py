# Importing Libraries
import json, yaml
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# Reading Configurations
with open("configurations.yaml") as f:
    configurations = yaml.safe_load(f)

# Base64-decoded key and salt
key = base64.b64decode(configurations["rpc"]["key"])
salt = base64.b64decode(configurations["rpc"]["salt"])

def decrypt_data(encrypted_json):
    try:
        encrypted_data = json.loads(encrypted_json)
        iv = base64.b64decode(encrypted_data["iv"])
        ct = base64.b64decode(encrypted_data["ct"])
        tag = base64.b64decode(encrypted_data["tag"])

        # Set up the decryption cipher context
        cipher = Cipher(algorithms.AES(key), modes.GCM(iv, tag), backend=default_backend())
        decryptor = cipher.decryptor()

        # Decrypt the ciphertext
        decrypted_data = decryptor.update(ct) + decryptor.finalize()

        return json.loads(decrypted_data)
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    print("File For Decrypting Data")
