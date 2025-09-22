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
    

def transform_data_for_chart(attribute_data_out):    
    if attribute_data_out.get("error"):
        return {
            "attributeName": attribute_data_out["attributeName"],
            "error": attribute_data_out["error"]
        }
    else:
        decoded_data = decode_response(attribute_data_out["data"])
    
        if not decoded_data:
            return {
                "attributeName": attribute_data_out["attributeName"],
                "error": "Could not decode response"
            }
    
        data_dictionary = json.loads(decoded_data["data"])
        
        columns = data_dictionary["columns"]
        rows = data_dictionary["rows"]
        
        return{
            "attributeName": attribute_data_out["attributeName"],
            "columns": columns,
            "rows": rows
        }
        
        # records = [dict(zip(columns, row)) for row in rows]
        # print(records)
    
