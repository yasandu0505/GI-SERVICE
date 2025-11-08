from traceback import print_tb
from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
from datetime import datetime
import json
import binascii
from google.protobuf.wrappers_pb2 import StringValue
import time
import asyncio
import aiohttp
from datetime import datetime
from collections import defaultdict
from typing import Sequence

class IncomingServiceAttributes:
    def __init__(self, config : dict):
        self.config = config
    
    def _normalize_timestamp(self, time_stamp: str | None) -> str | None:
        """
        Ensure timestamp is in ISO format expected by downstream services.
        Accepts dates like '2022-05-05' and converts to '2022-05-05T00:00:00Z'.
        """
        if not time_stamp:
            return time_stamp

        ts = time_stamp.strip()

        try:
            # Convert to standard ISO with Z suffix
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            # Fallback for date without time component
            if "T" not in ts:
                return f"{ts}T00:00:00Z"
            if ts.endswith("Z"):
                return ts
            return f"{ts}Z"
            
    async def expose_relevant_attributes(self, ENTITY_PAYLOAD: ENTITY_PAYLOAD , entityId, session: aiohttp.ClientSession):
        
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
            async with session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                attributes = await response.json()

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
                    search_url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
            
                    search_payload = {
                        "id": item["id"],
                        "kind": {
                            "major": "",
                            "minor": ""
                            },
                        "name": "",
                        "created": "",
                        "terminated": ""
                    }
            
                    try:
                        async with session.post(search_url, json=search_payload, headers=headers) as response:
                            response.raise_for_status()
                            output = await response.json()

                        item["name"] =  output["body"][0]["name"]
                        decoded_name = self.decode_protobuf_attribute_name(item["name"])
                        metadata_url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/metadata"

                        try:
                            async with session.get(metadata_url, headers=headers) as response:
                                response.raise_for_status()
                                metadata = await response.json()

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
        
    async def expose_data_for_the_attribute(self, ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD , entityId, session: aiohttp.ClientSession):
        global_start_time = time.perf_counter()
        nameCode = ATTRIBUTE_PAYLOAD.nameCode
        
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/attributes/{nameCode}"
        
        headers = {
            "Content-Type": "application/json",
        }
        
        actualName = None
        
        try:
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
                        kind_minor = parent_entity.get("kind", {}).get("minor")
                        if kind_minor == "department" or kind_minor == "minister":
                            return parent_entity
                        else:
                            return await self.find_parent_department(session, related_id)
        else:
            parent_entity = await self.get_node_data_by_id(entity_id, session)
            if parent_entity:
                kind_minor = parent_entity.get("kind", {}).get("minor")
                if kind_minor == "department" or kind_minor == "minister":
                    return parent_entity
                

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
        
    async def expose_category_by_id(self, id: str | None, session: aiohttp.ClientSession):

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

                async with session.post(url, json=payload_dataset, headers=headers) as response:
                    response.raise_for_status()
                    res_json = await response.json()
                    response_list = res_json.get("body",[])
                    
                    for item in response_list:
                        item["nameExact"] = None
                        item["name"] = self.decode_protobuf_attribute_name(item["name"])
                    
                    finalOutput["categories"] = response_list        
                            
            else:       
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
                        metadata_parent = await self.get_metadata_for_entity(session, id)
                        if(metadata_parent):
                            item["nameExact"]= self.decode_protobuf_attribute_name(metadata_parent[item["name"]]) 
                        if(parent_department):
                            source = self.decode_protobuf_attribute_name(parent_department["name"])
                            item["source"] = source
                            item["sourceId"] = parent_department["id"]
                            item["sourceType"] = parent_department["kind"].get("minor")
                    
                    if kind == "Category":
                            if(parent_department):
                                source = self.decode_protobuf_attribute_name(parent_department["name"])
                                item["source"] = source
                                item["sourceId"] = parent_department["id"]
                                item["sourceType"] = parent_department["kind"].get("minor")
                                
                            finalOutput["categories"].append(item)
                 
        except Exception as e:
            return {"error": f"Failed to fetch categories or attributes {id}: {str(e)}"}
        
        global_end_time = time.perf_counter()
        global_elapsed_time = global_end_time - global_start_time
        print(f"\n Total time taken: {global_elapsed_time:.4f} seconds")
        return finalOutput   
        
    async def datacategoriesbyyear(self, name: str | None, parentId: str, session: aiohttp.ClientSession):
        global_start_time = time.perf_counter()
        finalOutput = {"datasets": defaultdict(list)}

        try:
            if not name:
                return finalOutput

            # Step 1: Fetch relations
            relation_results = await asyncio.gather(
                self.fetch_relation(session, parentId, "IS_ATTRIBUTE"),
                return_exceptions=True
            )

            searchList = [
                item
                for sublist in relation_results
                if isinstance(sublist, list)
                for item in sublist
            ]

            # Step 2: Get node data by ID
            node_data_results = await asyncio.gather(
                *[self.get_node_data_by_id(item['relatedEntityId'], session) for item in searchList],
                return_exceptions=True
            )

            # Step 3: Get parent department
            parent_department = await self.find_parent_department(session, parentId)

            # Step 4: Get metadata once
            metadata_parent = await self.get_metadata_for_entity(session, parentId)

            for item in node_data_results:
                if isinstance(item, Exception):
                    continue

                kind = item.get("kind", {}).get("major", "")
                raw_name = item.get("name")
                decoded_name = self.decode_protobuf_attribute_name(raw_name)
                
                if decoded_name != name:
                    continue

                # Set base fields
                item["name"] = decoded_name
                item["nameExact"] = None
                item["parentId"] = parentId
                item["source"] = ""

                if kind == "Dataset":
                    created_date = item.get("created")
                    year = created_date.split("-")[0] if created_date else "unknown"
                    finalOutput["datasets"][year].append(item)

                    # Handle metadata_parent mapping
                    if metadata_parent and decoded_name in metadata_parent:
                        item["nameExact"] = self.decode_protobuf_attribute_name(metadata_parent[decoded_name])

                    # Set source from parent department
                    if parent_department:
                        item["source"] = self.decode_protobuf_attribute_name(parent_department.get("name", ""))

        except Exception as e:
            return {"error": f"Failed to fetch categories or attributes for parentId {parentId}: {str(e)}"}

        global_end_time = time.perf_counter()
        print(f"\nTotal time taken: {global_end_time - global_start_time:.4f} seconds")
        return finalOutput
               
    async def get_active_ministers(self, entityId, dateActive, session):
        normalized_timestamp = self._normalize_timestamp(dateActive)
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/relations"
        
        headers = {"Content-Type": "application/json"}
        
        payload = {
            "name": "AS_MINISTER",
            "activeAt": normalized_timestamp,
            "direction": "OUTGOING"
        }

        activeMinisterIds = []

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                res_json = await response.json()
                # print(f"Res JSON: {res_json}")
                
                # Handle both direct list and wrapped response
                if isinstance(res_json, dict):
                    response_list = res_json.get("body", [])
                else:
                    response_list = res_json
                
                # print(f"Response list: {response_list}")
                for item in response_list:
                    activeMinisterIds.append(item["relatedEntityId"])

        except Exception as e:
            return {"error": f"Failed to get active ministers for {entityId}: {str(e)}"}

        return activeMinisterIds

    async def get_active_departments(self, entityId, dateActive, session):
        normalized_timestamp = self._normalize_timestamp(dateActive)
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": "AS_DEPARTMENT",
            "activeAt": normalized_timestamp,
            "direction": "OUTGOING"
        }
        
        activeDepartments = []

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                res_json = await response.json()
                
                # Handle both direct list and wrapped response
                if isinstance(res_json, dict):
                    response_list = res_json.get("body", [])
                else:
                    response_list = res_json
                
                for item in response_list:
                    activeDepartments.append({
                        "ministerId": entityId,
                        "departmentId": item["relatedEntityId"]
                    })

        except Exception as e:
            return {"error": f"Failed to get active departments for {entityId}: {str(e)}"}

        return activeDepartments

    async def get_ministers_and_departments(self, entityId, dateActive, session):
        """
        Get active ministers and their departments for a given entity and timestamp.
        
        Step 1: Get active ministers
        Step 2: For each minister, get their departments in parallel
        """

        departments_results = []

        try:
            # Step 1: Get active ministers (sequential, must happen first)
            minister_ids = await self.get_active_ministers(entityId, dateActive, session)

            print(f"Minister IDs: {minister_ids}")

            if len(minister_ids) == 0:
                return departments_results

                        
            # Step 2: Get departments for each minister in parallel
            tasks_for_departments = [
                self.get_active_departments(minister_id, dateActive, session)
                for minister_id in minister_ids
            ]
            
            # Execute all department fetches in parallel
            departments_results = await asyncio.gather(*tasks_for_departments, return_exceptions=True)

            # Step 3: Flatten the nested list
            flattened_results = []
            for result in departments_results:
                if isinstance(result, list):
                    flattened_results.extend(result)

            print(f"Flattened results length: {len(flattened_results)}")
            
            return flattened_results
            
        except Exception as e:
            return {"error": f"Failed to get ministers and departments for {entityId}: {str(e)}"}

    async def get_sankey_data(self,session, entityId, dates: Sequence[str]):
        # first assume theres only two dates
        
        tasks_for_dates = [
            self.get_ministers_and_departments(entityId, date, session)
            for date in dates
        ]
        dates_gov_struct = await asyncio.gather(*tasks_for_dates, return_exceptions=True)

        # example format of dates_gov_struct:
        # [
        #     [
        #         {"ministerId": "minister-123", "departmentId": "dept-456"},
        #         {"ministerId": "minister-123", "departmentId": "dept-457"},
        #         {"ministerId": "minister-456", "departmentId": "dept-789"},
        #     ],
        #     [
        #         {"ministerId": "minister-321", "departmentId": "dept-654"}
        #     ]
        # ]
        
        # create comparison dictionary of departments by ministers
        departments_by_ministers = {}
        expected_slots = len(dates)
        nodes: list[dict[str, str]] = []
        node_indices: dict[tuple[str, int], int] = {} # key: (minister_id, date_index), value: node_index

        for date_index, result in enumerate(dates_gov_struct):
            if isinstance(result, Exception):
                continue

            if isinstance(result, dict) and "error" in result:
                continue

            if not isinstance(result, list):
                continue

            for relation in result:
                if not isinstance(relation, dict):
                    continue

                department_id = relation.get("departmentId")
                minister_id = relation.get("ministerId")

                if not department_id:
                    continue

                # create nodes dict
                node_index = None
                if minister_id:
                    node_key = (minister_id, date_index)
                    node_index = node_indices.get(node_key)
                    if node_index is None:
                        node_index = len(nodes)
                        node_indices[node_key] = node_index
                        nodes.append({
                            "name": minister_id,
                            "time": dates[date_index]
                        })

                # create departments_by_ministers dict for comparison:
                #  key is the department
                # value is the ministers index in the nodes list (for each date)
                timeline = departments_by_ministers.get(department_id) # check if dept already in dict
                if timeline is None:
                    timeline = [None] * expected_slots
                    departments_by_ministers[department_id] = timeline

                timeline[date_index] = node_index
                
                
            
        print(f"Nodes: {nodes}")
        return departments_by_ministers

