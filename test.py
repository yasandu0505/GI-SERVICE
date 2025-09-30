# x = "Legal Division Performance"

# def str2ascii(s):
#     return [ord(c) for c in s]

# print(str2ascii(x))

import requests

def get_attribute_data(entityId, attribute_name):
    url = f"http://0.0.0.0:8081/v1/entities/{entityId}/attributes/{attribute_name}"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    res = response.json()
    print(res)
        
    return res

import json
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

def decode_response(response):
    """
    Decode the protobuf response and extract the data
    """
    response_value = response["value"]
        
    # Extract the hex string
    hex_value = json.loads(response_value)["value"]
    
    # Convert hex string to bytes
    proto_bytes = bytes.fromhex(hex_value)
    
    # Parse into a Protobuf Struct
    struct_obj = struct_pb2.Struct()
    struct_obj.ParseFromString(proto_bytes)
    
    # Convert to a Python dictionary
    return MessageToDict(struct_obj)

res = get_attribute_data("2153_12_dep_165", "3ee0fd7e69")  # Example entityId and attribute_name
decoded_data = decode_response(res)
print("\n")
print(decoded_data)