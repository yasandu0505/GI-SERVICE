# from src.utils import CacheService
import requests
from datetime import datetime

class IncomingServiceOrgchart:
    # def __init__(self, cache : CacheService ):
    #     self.cache = cache
        
    async def get_data(self):   
        ministries = await self.cache.get_ministries()
        departments = await self.cache.get_departments()
        people = await self.cache.get_people()
        
        return {
            "ministries" : ministries["body"],
            "departments" : departments["body"],
            "people" : people["body"]
        }
        
    async def get_documents(self):
        
        url = "https://aaf8ece1-3077-4a52-ab05-183a424f6d93-dev.e1-us-east-azure.choreoapis.dev/data-platform/query-api/v1.0/v1/entities/search"

        payload = {
            "id": "",
            "kind": {
                "major": "Document",
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
            documents = response.json()
            documents_out = []
            for item in documents["body"]:
                documents_out.append({
                    "id" : item["id"],
                    "created" : item["created"]
                })
                
        except Exception as e:
            documents_out = {
                "error": str(e)
                }
        
        return documents_out

    async def get_presidents(self):
           
        url = "https://aaf8ece1-3077-4a52-ab05-183a424f6d93-dev.e1-us-east-azure.choreoapis.dev/data-platform/query-api/v1.0/v1/entities/gov_01/relations"

        payload = {
            "id": "",
            "relatedEntityId": "",
            "name": "AS_PRESIDENT",
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
            presidents = response.json()
            president_details_out = []
            
            for item in presidents:
                
                url = "https://aaf8ece1-3077-4a52-ab05-183a424f6d93-dev.e1-us-east-azure.choreoapis.dev/data-platform/query-api/v1.0/v1/entities/search"
                
                payload = {
                    "id": item["relatedEntityId"],
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
                    president_details = response.json()
                    president_details_out.append({
                        "id" : item["relatedEntityId"],
                        "name" : president_details["body"][0]["name"],
                        "startTime" : item["startTime"],
                        "endTime" : item["endTime"]
                    })
                except Exception as e:
                    president_details_out.append({
                        "id" : item["relatedEntityId"],
                        "name" : f"error : {str(e)}",
                        "startTime" : item["startTime"],
                        "endTime" : item["endTime"]
                    })
            
        except Exception as e:
            president_details_out = {
                "error": str(e)
                }

        return president_details_out
    
    def get_timeline(self, documentData, presidentData):
        time_line_out = []
        for president in presidentData:
            start = datetime.fromisoformat(president["startTime"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(president["endTime"].replace("Z", "+00:00")) if president["endTime"] else None
            
            valid_dates = []
            
            for date in documentData:
                date_created = datetime.fromisoformat(date["created"].replace("Z", "+00:00"))
                
                if end:
                    if start <= date_created < end:
                        valid_dates.append(date["created"])
                else:
                    if date_created >= start:
                        valid_dates.append(date["created"])
        
            valid_dates = sorted(valid_dates ,key=lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ"))
            time_line_out.append({
                "id" : president["id"],
                "name" : president["name"],
                "date_range" : valid_dates
                })
        
        return time_line_out
    
    
    
        