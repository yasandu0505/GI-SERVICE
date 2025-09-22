from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
import requests
from datetime import datetime

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
            api_output = response.json()

            for item in api_output:
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
                        
            api_output = data_list_for_req_year
                        
            if len(api_output) == 0:
                return {
                    "year": req_year,
                    "attributes": {
                        "message": "No data found"
                    }
                }

            for item in api_output:
                                
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
                    api_output_2 = response.json()
                    item["name"] =  api_output_2["body"][0]["name"]
                except Exception as e:
                    item["name"] = f"error : {str(e)}"
                    
        except Exception as e:
            api_output = {"error": str(e)}
            
        return {
            "year": req_year,
            "attributes": api_output
        }
    
    
    def expose_data_for_the_attribute(self, ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD , entityId):
        attribute_name = ATTRIBUTE_PAYLOAD.attribute_name
        
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/attributes/{attribute_name}"
        
        headers = {
            "Conten-Type": "application/json",
            # "Authorization": f"Bearer {token}"    
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  
            api_output = response.json()
            
            if len(api_output) == 0:
                api_output = {
                    "message": "No data found"
                    }

        except Exception as e:
            api_output = {"error": str(e)}
        return api_output
        
