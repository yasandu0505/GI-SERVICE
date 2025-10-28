from traceback import print_tb
from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
import requests
from datetime import datetime
import json
import binascii
from google.protobuf.wrappers_pb2 import StringValue
import time
import asyncio
import aiohttp
from datetime import datetime
from collections import defaultdict

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
            response = requests.post(url, json=payload, headers=headers, timeout=(30, 90))
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
                        response = requests.post(url, json=payload, headers=headers, timeout=(30, 90))
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
                            response = requests.get(url, headers=headers, timeout=(30, 90))
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
            data = json.loads(name)
            hex_value = data.get("value")
            if not hex_value:
                return ""

            decoded_bytes = binascii.unhexlify(hex_value)
            sv = StringValue()
            try:
                sv.ParseFromString(decoded_bytes)
                if(sv.value.strip() == ""):
                    return decoded_bytes.decode("utf-8", errors="ignore").strip()
                return sv.value.strip()
            except Exception:
                decoded_str = decoded_bytes.decode("utf-8", errors="ignore")
                cleaned = ''.join(ch for ch in decoded_str if ch.isprintable())
                return cleaned.strip()
        except Exception as e:
            print(f"[DEBUG decode] outer exception: {e}")
            return ""
        
    async def expose_data_for_the_attribute(self, ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD , entityId):
        global_start_time = time.perf_counter()
        nameCode = ATTRIBUTE_PAYLOAD.nameCode
        
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/attributes/{nameCode}"
        
        headers = {
            "Content-Type": "application/json",
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    parent_metadata = await self.get_metadata_for_entity(session, entityId)
                    response.raise_for_status()  
                    attribute_data = await response.json()
                    
            actualName = self.decode_protobuf_attribute_name(parent_metadata[nameCode])
            
            if len(attribute_data) == 0:
                return {
                    "attributeName": actualName,
                    "error": "No data found"
                } 
                
            global_end_time = time.perf_counter()
            global_elapsed_time = global_end_time - global_start_time
            print(f"\n Total time taken: {global_elapsed_time:.4f} seconds")         
            
            return{
                "attributeName": actualName,
                "data": attribute_data
            }

        except Exception as e:
            return{
                "attributeName": actualName,
                "error": f"No data found - Error occured - {str(e)}"
            }
    
    async def fetch_relation(self,session, id, relationName):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{id}/relations"
        headers = {"Content-Type": "application/json"}  
        payload = {
            "relatedEntityId": "",
            "startTime": "",
            "endTime": "",
            "id": "",
            "name": relationName,
            "activeAt": "",
            "direction": "OUTGOING",
        }
        async with session.post(url, json=payload, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    
    async def get_node_data_by_id(self,entityId, session):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        payload = {
            "id": entityId
        }
        headers = {"Content-Type":"application/json"}
        
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                res_json = await response.json()
                response_list = res_json.get("body",[])
                return response_list[0]
                    
        except Exception as e:
            return {"error": f"Failed to fetch entity data by id {entityId}: {str(e)}"}     

    async def find_parent_department(self, session, entity_id):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entity_id}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "relatedEntityId": "",
            "startTime": "",
            "endTime": "",
            "id": "",
            "name": "AS_CATEGORY",
            "activeAt": "",
            "direction": "INCOMING"
        }

        async with session.post(url, json=payload, headers=headers) as response:
            response.raise_for_status()
            relations = await response.json()

        if relations:
            for rel in relations:
                related_id = rel.get("relatedEntityId")
                if related_id:
                    parent_entity = await self.get_node_data_by_id(related_id, session)
                    if parent_entity:
                        kind_major = parent_entity.get("kind", {}).get("minor")
                        if kind_major == "department" or kind_major == "minister":
                            return parent_entity
                        else:
                            return await self.find_parent_department(session, related_id)

        return None
      
    async def get_metadata_for_entity(self, session, entityId):
        
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/metadata"
                
        headers = {
            "Content-Type": "application/json",
        }
                
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data
        except Exception as e:
            return f"failed to get metadata for {entityId} : {e}" 
        
    async def expose_category_by_id(self, id: str | None):

        global_start_time = time.perf_counter()
        
        headers = {"Content-Type": "application/json"}        
        finalOutput = { "categories": [], "datasets": defaultdict(list) }
        
        try:
            if id is None:
                searchList = []
                url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
                payload_dataset = {
                    "kind": {
                        "major": "Category",
                        "minor": "parentCategory"
                    }
                }

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload_dataset, headers=headers) as response:
                        response.raise_for_status()
                        res_json = await response.json()
                        response_list = res_json.get("body",[])
                        
                        for item in response_list:
                            item["nameExact"] = None
                            item["name"] = self.decode_protobuf_attribute_name(item["name"])
                        
                        finalOutput["categories"] = response_list        
                            
            else:       
                async with aiohttp.ClientSession() as session:
                    tasks_for_relations = [
                        self.fetch_relation(session, id, relationName)
                        for relationName in ["IS_ATTRIBUTE","AS_CATEGORY"]
                    ]
                    results = await asyncio.gather(*tasks_for_relations, return_exceptions=True)

                    searchList = [
                        item
                        for sublist in results
                        if isinstance(sublist, list)
                        for item in sublist
                    ]
                          
                async with aiohttp.ClientSession() as session:
                    tasks_for_entity_data = [
                        self.get_node_data_by_id(item['relatedEntityId'],session)
                        for item in searchList
                    ]
                    results = await asyncio.gather(*tasks_for_entity_data, return_exceptions=True)
                    
                    parent_department = await self.find_parent_department(session, id)    
                    
                    for item in results:
                        kind = item.get("kind", {}).get("major", "")
                        name = item.get("name")
                        name = self.decode_protobuf_attribute_name(name)

                        item["name"] = name
                        item["nameExact"] = None
                        item["parentId"] = id
                        item["source"] = ""
                        
                        if kind == "Dataset":
                            created_date = item.get("created")
                            if created_date:
                                year = created_date.split("-")[0]
                            else:
                                year = "unknown"
                            finalOutput["datasets"][year].append(item)
                            async with aiohttp.ClientSession() as session:
                                metadata_parent = await self.get_metadata_for_entity(session, id) 
                                if(metadata_parent):
                                    item["nameExact"]= self.decode_protobuf_attribute_name(metadata_parent[item["name"]]) 
                                source = self.decode_protobuf_attribute_name(parent_department["name"])
                                item["source"] = source
                        
                        elif kind == "Category":
                            finalOutput["categories"].append(item)
                 
        except Exception as e:
            return {"error": f"Failed to fetch categories or attributes {id}: {str(e)}"}
        
        global_end_time = time.perf_counter()
        global_elapsed_time = global_end_time - global_start_time
        print(f"\n Total time taken: {global_elapsed_time:.4f} seconds")
        return finalOutput   
        
            
    
