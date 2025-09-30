from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
import requests
from datetime import datetime
import json
import binascii
from google.protobuf.wrappers_pb2 import StringValue
import string
import re

class IncomingServiceAttributes:
    def __init__(self, config : dict):
        self.config = config
            
    async def expose_relevant_attributes(self, ENTITY_PAYLOAD: ENTITY_PAYLOAD , entityId):
        
        data_list_for_req_year = []
        req_entityId = entityId
        req_year = ENTITY_PAYLOAD.year
       
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{req_entityId}/relations"
        
        payload = {
            "id": "",
            "relatedEntityId": "",
            "name": "IS_ATTRIBUTE",
            "activeAt": "",
            "startTime": "",
            "endTime": "",
            "direction": ""
        }

        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {token}"  
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  
            attributes = response.json()
            
            if len(attributes) > 0:
                for item in attributes:
                    startTime = item["startTime"]
                    if "endTime" in item and item["endTime"]:
                        endTime = item["endTime"]
                    else:
                        endTime = startTime
                    if startTime and endTime:
                        start_year = datetime.fromisoformat(startTime.replace("Z", "")).year
                        end_year = datetime.fromisoformat(endTime.replace("Z", "")).year

                        # Check if req_year is between start and end year
                        if int(start_year) <= int(req_year) <= int(end_year):
                            data_list_for_req_year.append({
                                "id" : item["relatedEntityId"],
                                "startTime" : item["startTime"],
                                "endTime" : item["endTime"]
                            })  
                
                if len(data_list_for_req_year) == 0:
                    return {
                        "year": req_year,
                        "attributes": {
                            "message": "No attributes found in the requested time range"
                        }
                    } 
                
                for item in data_list_for_req_year:
                    url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
                
                    payload = {
                        "id": item["id"],
                        "kind": {
                            "major": "",
                            "minor": ""
                            },
                        "name": "",
                        "created": "",
                        "terminated": ""
                    }
                
                    headers = {
                        "Content-Type": "application/json",
                        # "Authorization": f"Bearer {token}"  
                    }
                
                    try:
                        response = requests.post(url, json=payload, headers=headers)
                        response.raise_for_status()  
                        output = response.json()
                        item["name"] =  output["body"][0]["name"]
                        print(item["name"])
                        decoded_name = self.decode_protobuf_attribute_name(item["name"])
                        print(f"Decoded name : {decoded_name}")
                        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/metadata"
                        headers = {
                            "Content-Type": "application/json",
                            # "Authorization": f"Bearer {token}"  
                        }
                        try:
                            response = requests.get(url, headers=headers)
                            response.raise_for_status()  
                            metadata = response.json()
                            for k, v in metadata.items():
                                if k == decoded_name:
                                    item["human_readable_name"] = v
                                    break
                        except Exception as e:
                            metadata = {}
                            print(f"Error fetching metadata: {str(e)}")
                            item["human_readable_name"] = "No description available"
                            
                    except Exception as e:
                        item["name"] = f"error : {str(e)}"
            else:
                return {
                    "year": req_year,
                    "attributes": {
                        "message": "No attributes found for the entity"
                    }
                }
                               
        except Exception as e:
            return {
                "year": req_year,
                "attributes": {
                    "error": str(e)
                }
            }
        
        return {
            "year": req_year,
            "attributes": data_list_for_req_year
        }
    
    def decode_protobuf_attribute_name(self, name : str) -> str:
        try:
            print(f"[DEBUG decode] input name: {name!r}")
            data = json.loads(name)
            hex_value = data.get("value")
            print(f"[DEBUG decode] hex_value: {hex_value!r}")
            if not hex_value:
                return ""

            decoded_bytes = binascii.unhexlify(hex_value)
            sv = StringValue()
            try:
                sv.ParseFromString(decoded_bytes)
                return sv.value.strip()
            except Exception:
                decoded_str = decoded_bytes.decode("utf-8", errors="ignore")
                cleaned = ''.join(ch for ch in decoded_str if ch.isprintable())
                return cleaned.strip()
        except Exception as e:
            print(f"[DEBUG decode] outer exception: {e}")
            return ""
        
    def expose_data_for_the_attribute(self, ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD , entityId):
        attribute_name_readable = ATTRIBUTE_PAYLOAD.attribute_name
        attribute_name = ""
        
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/metadata"
        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {token}"  
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  
            metadata = response.json()
            
            print(f"Metadata fetched: {metadata}")
            
            for key, value in metadata.items():
                decoded_str = self.decode_protobuf_attribute_name(value)
                decoded_str_clean = decoded_str.strip().strip(string.punctuation)                
                if decoded_str_clean == attribute_name_readable:
                    print(f"Found attribute name : {key}")
                    attribute_name = key
                    break
                
            if not attribute_name:
                return {
                    "attributeName": attribute_name_readable,
                    "error": "Attribute not found in metadata"
                }
        except Exception as e:
            metadata = {}
            print(f"Error fetching metadata: {str(e)}")
            
        
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/attributes/{attribute_name}"
        
        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {token}"    
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  
            attribute_data = response.json()
            
            if len(attribute_data) == 0:
                return {
                    "attributeName": attribute_name_readable,
                    "error": "No data found"
                } 
            
            return{
                "attributeName": attribute_name_readable,
                "data": attribute_data
            }

        except Exception as e:
            return{
                "attributeName": attribute_name_readable,
                "error": f"No data found - Error occured - {str(e)}"
            }
    

    def expose_all_attributes_v2(self):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        
        payload = {
            "kind": {
                "major": "Dataset",
                "minor": "tabular"
            },
        }
        
        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {token}"    
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  
            all_attributes = response.json()
            
            # access only the body
            body = all_attributes.get('body', [])
            
            if len(body) == 0:
                return {
                    "message": "No attributes found"
                } 
            
            # Group simplified items (id, name, created) by year extracted from 'created'
            grouped_by_year = {}
            
            for item in body:
                item_id = item.get("id") or item.get("entityId") or ""
                raw_name = item.get("name", "")
                hash_to_the_attribute_name = self.decode_protobuf_attribute_name(raw_name)
                sliced_id = item_id.split("_attr")[0]
                
                url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{sliced_id}/metadata"
                
                headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()  
                    metadata = response.json()
                    for key, value in metadata.items():
                        if key == hash_to_the_attribute_name:
                            attribute_name_to_decode = value
                            decoded_name = self.decode_protobuf_attribute_name(attribute_name_to_decode)
                            break
                except Exception as e:
                    metadata = {}
                    print(f"Error fetching metadata: {str(e)}")
                    decoded_name = "No description available"   
                
                created = item.get("created", "") or item.get("createdAt", "")
                
                year_key = "unknown"
                if created:
                    try:
                        year_key = str(datetime.fromisoformat(created.replace("Z", "")).year)
                    except Exception:
                        m = re.search(r"\b(20\d{2}|19\d{2})\b", created)
                        if m:
                            year_key = m.group(0)
                
                simplified = {
                    "id": item_id,
                    "parent_entity_id": sliced_id,
                    "attribute_hash_name": hash_to_the_attribute_name,
                    "name": decoded_name,
                    "created": created
                }
                grouped_by_year.setdefault(year_key, []).append(simplified)
            
            return {
                "attributes": grouped_by_year
            }
        except Exception as e:
            return{
                "error": f"Error occured - {str(e)}"
            }

    
    def expose_all_attributes_v1(self):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        
        payload = {
            "kind": {
                "major": "Dataset",
                "minor": "tabular"
            }
        }
        
        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {token}"    
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  
            all_attributes = response.json()
            
            # access only the body
            body = all_attributes.get('body', [])
            
            if len(body) == 0:
                return {
                    "message": "No attributes found"
                } 
            
            # Group simplified items (id, name, created) by year extracted from 'created'
            grouped_by_year = {}
            
            for item in body:
                item_id = item.get("id") or item.get("entityId") or ""
                raw_name = item.get("name", "")
                hash_to_the_attribute_name = self.decode_protobuf_attribute_name(raw_name)
                sliced_id = item_id.split("_attr")[0]
                
                url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{sliced_id}/metadata"
                
                headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()  
                    metadata = response.json()
                    for key, value in metadata.items():
                        if key == hash_to_the_attribute_name:
                            attribute_name_to_decode = value
                            decoded_name = self.decode_protobuf_attribute_name(attribute_name_to_decode)
                            break
                        
                        # if key == "parent_of_parent_category_id":
                        #     print(f"Found parent_of_parent_category_id: {value}")
                        #     break   

                except Exception as e:
                    metadata = {}
                    print(f"Error fetching metadata: {str(e)}")
                    decoded_name = "No description available"   
                
                created = item.get("created", "") or item.get("createdAt", "")
                
                year_key = "unknown"
                if created:
                    try:
                        year_key = str(datetime.fromisoformat(created.replace("Z", "")).year)
                    except Exception:
                        m = re.search(r"\b(20\d{2}|19\d{2})\b", created)
                        if m:
                            year_key = m.group(0)
                
                parent_of_parent_category_id = metadata.get("parent_of_parent_category_id", "N/A")
                
                if parent_of_parent_category_id == "N/A":
                    decoded_parent_of_parent = "N/A"
                else:   
                    decoded_parent_of_parent = self.decode_protobuf_attribute_name(parent_of_parent_category_id) 
                    print(f"Decoded parent_of_parent_category_id: {decoded_parent_of_parent}")
                    
                    url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        
                    payload = {
                            "id": decoded_parent_of_parent
                    }
                    
                    headers = {
                        "Content-Type": "application/json",
                        # "Authorization": f"Bearer {token}"    
                    }
                    
                    try:
                        response = requests.post(url, json=payload, headers=headers)
                        response.raise_for_status()  
                        parent_entity = response.json()
                                                
                        parent_body = parent_entity.get('body', [])
                        
                        print(f"Parent entity body: {parent_body}")
                        
                        if len(parent_body) > 0:
                            parent_raw_name = parent_body[0].get("name", "")
                            print(f"Parent raw name: {parent_raw_name}")
                            decoded_parent_of_parent = self.decode_protobuf_attribute_name(parent_raw_name)
                            print(f"Decoded parent_of_parent_category_id: {decoded_parent_of_parent}")
                        else:
                            decoded_parent_of_parent = "N/A"        
                    except Exception as e:
                        print(f"Error fetching parent entity: {str(e)}")
                        decoded_parent_of_parent = "N/A"
                        
                    
                simplified = {
                    "id": item_id,
                    "parent_of_parent_category_id": decoded_parent_of_parent,
                    "parent_entity_id": sliced_id,
                    "attribute_hash_name": hash_to_the_attribute_name,
                    "name": decoded_name,
                    "created": created
                }
                grouped_by_year.setdefault(year_key, []).append(simplified)
            
            return {
                "attributes": grouped_by_year,
                "lenght" : len(grouped_by_year["2022"])
            }

        except Exception as e:
            return{
                "error": f"Error occured - {str(e)}"
            }
    
    def expose_all_attributes(self):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        
        payload = {
            "kind": {
                "major": "Dataset",
                "minor": "tabular"
            }
        }
        
        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {token}"    
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  
            all_attributes = response.json()
            
            # access only the body
            body = all_attributes.get('body', [])
            
            if len(body) == 0:
                return {
                    "message": "No attributes found"
                } 
            
            # Group simplified items (id, name, created) by year extracted from 'created'
            grouped_by_year = {}
            
            for item in body:
                item_id = item.get("id") or item.get("entityId") or ""
                raw_name = item.get("name", "")
                hash_to_the_attribute_name = self.decode_protobuf_attribute_name(raw_name)
                sliced_id = item_id.split("_attr")[0]
                
                url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{sliced_id}/metadata"
                
                headers = {
                    "Content-Type": "application/json",
                    # "Authorization": f"Bearer {token}"  
                }
                
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()  
                    metadata = response.json()
                    for key, value in metadata.items():
                        if key == hash_to_the_attribute_name:
                            attribute_name_to_decode = value
                            decoded_name = self.decode_protobuf_attribute_name(attribute_name_to_decode)
                            break
  
                except Exception as e:
                    metadata = {}
                    print(f"Error fetching metadata: {str(e)}")
                    decoded_name = "No description available"   
                
                created = item.get("created", "") or item.get("createdAt", "")
                
                year_key = "unknown"
                if created:
                    try:
                        year_key = str(datetime.fromisoformat(created.replace("Z", "")).year)
                    except Exception:
                        m = re.search(r"\b(20\d{2}|19\d{2})\b", created)
                        if m:
                            year_key = m.group(0)
                
                
                if "dep" in sliced_id:
                    parent_of_parent_category_id = sliced_id
                    print(parent_of_parent_category_id)
                else:
                    related_parent = {} 
                    url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{sliced_id}/relations"
                
                    payload = {
                        "id": "",
                        "relatedEntityId": "",
                        "name": "AS_CATEGORY",
                        "activeAt": "",
                        "startTime": "",
                        "endTime": "",
                        "direction": "INCOMING"
                    }
                    
                    headers = {
                        "Content-Type": "application/json",
                        # "Authorization": f"Bearer {token}"  
                    }
                    
                    try:
                        response = requests.post(url, json=payload, headers=headers)
                        response.raise_for_status()  
                        related_parent = response.json()
                        print(related_parent)
                            
                    except Exception as e:
                        print(f"Error fetching related_parent: {str(e)}")
                        decoded_name = "No description available"  
                        

                    # Check if it's a list and has items
                    if isinstance(related_parent, list) and len(related_parent) > 0:
                        related_entity_id = related_parent[0].get('relatedEntityId')
                        
                        if related_entity_id:
                            url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{related_entity_id}/relations"
                            
                            headers = {
                                "Content-Type": "application/json",
                            }
                            
                            payload = {
                                "id": "",
                                "relatedEntityId": "",
                                "name": "AS_CATEGORY",
                                "activeAt": "",
                                "startTime": "",
                                "endTime": "",
                                "direction": "INCOMING"
                            }
                            
                            try:
                                response = requests.post(url, json=payload, headers=headers)
                                response.raise_for_status()  
                                parent_response = response.json()
                                print(f"parent response >>>{parent_response}")

                            except Exception as e:
                                print(f"Error fetching parent_response: {str(e)}")
                                parent_response = {}
                        else:
                            parent_of_parent_category_id = "N/A"
                    else:
                        parent_of_parent_category_id = "N/A"
                        
                          
                    if isinstance(parent_response, list) and len(parent_response) > 0:
                        parent_of_parent_category_id = parent_response[0].get("relatedEntityId")
                        print(parent_of_parent_category_id)
                    else:
                        parent_of_parent_category_id = "N/A"
                                          
                if parent_of_parent_category_id == "N/A":
                    decoded_parent_of_parent = "N/A"
                else:
                    # decoded_parent_of_parent = self.decode_protobuf_attribute_name(parent_of_parent_category_id)
                    decoded_parent_of_parent = parent_of_parent_category_id
                    
                    print(f"Decoded parent_of_parent_category_id: {decoded_parent_of_parent}")
                    
                    url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        
                    payload = {
                            "id": decoded_parent_of_parent
                    }
                    
                    headers = {
                        "Content-Type": "application/json",
                        # "Authorization": f"Bearer {token}"    
                    }
                    
                    try:
                        response = requests.post(url, json=payload, headers=headers)
                        response.raise_for_status()  
                        parent_entity = response.json()
                                                
                        parent_body = parent_entity.get('body', [])
                        
                        print(f"Parent entity body: {parent_body}")
                        
                        if len(parent_body) > 0:
                            parent_raw_name = parent_body[0].get("name", "")
                            print(f"Parent raw name: {parent_raw_name}")
                            decoded_parent_of_parent = self.decode_protobuf_attribute_name(parent_raw_name)
                            print(f"Decoded parent_of_parent_category_id: {decoded_parent_of_parent}")
                        else:
                            decoded_parent_of_parent = "N/A"        
                    except Exception as e:
                        print(f"Error fetching parent entity: {str(e)}")
                        decoded_parent_of_parent = "N/A"
                        
                    
                simplified = {
                    "id": item_id,
                    "parent_of_parent_category_id": decoded_parent_of_parent,
                    "parent_entity_id": sliced_id,
                    "attribute_hash_name": hash_to_the_attribute_name,
                    "name": decoded_name,
                    "created": created
                }
                grouped_by_year.setdefault(year_key, []).append(simplified)
            
            return {
                "attributes": grouped_by_year
                
            }

        except Exception as e:
            return{
                "error": f"Error occured - {str(e)}"
            }


