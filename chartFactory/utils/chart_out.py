from chartFactory.data_transformers.transformers_registry import TRANSFORMERS
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
    

def transform_data_for_chart(response, chart_type, x_axis, y_axis, label, value):
    decoded_data = decode_response(response)
    
    if not decoded_data:
        print("Could not decode response")
        return None
    
    data_dictionary = json.loads(decoded_data["data"])
        
    columns = data_dictionary["columns"]
    rows = data_dictionary["rows"]
    records = [dict(zip(columns, row)) for row in rows]
    
    transformer = TRANSFORMERS.get(chart_type)
    if not transformer:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    if chart_type == "line" or chart_type == "bar":
        return transformer(records, x_axis, y_axis, label)
    elif chart_type == "pie":
        return transformer(records, value, label)
    elif chart_type == "table":
        return transformer(records)
