from src.models import REQ_ONE
import requests
from datetime import datetime

class IncomingService:
    def incoming_payload_extractor(self, REQ_ONE: REQ_ONE , entityId ):
        year = REQ_ONE.year
        year = REQ_ONE.year
        govId = REQ_ONE.govId
        presidentId = REQ_ONE.presidentId
        dataSet = REQ_ONE.dataSet 
            
        return {
            "year" : year,
            "govId" : govId,
            "presidentId" : presidentId,
            "dataSet" : dataSet,
            "entityId" : entityId
        }
        
    def expose_relevant_attributes(self, extracted_data):
        
        data_list_for_req_year = []
        req_entityId = extracted_data["entityId"]
        req_year = extracted_data["year"]
        
        url = f"https://aaf8ece1-3077-4a52-ab05-183a424f6d93-dev.e1-us-east-azure.choreoapis.dev/data-platform/query-api/v1.0/v1/entities/{req_entityId}/relations"
        
        # TODO : I need to change this AS_DEPARTMENT to IS_ATTRIBUTE (After Vibhatha implements the thing)
        payload = {
            "id": "",
            "relatedEntityId": "",
            "name": "AS_DEPARTMENT",
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
                endTime = item["endTime"]
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
                api_output = {
                    "message": "No data found"
                    }

        except Exception as e:
            api_output = {"error": str(e)}

        return {
            "extracted_data": extracted_data,
            "api_output": api_output
        }
    
    
    
    
    
    
    # def query_aggregator(self, extracted_data):
    #     # Get years -----------------------------------------------
    #     data_list_for_req_year = []
    #     req_year = extracted_data["year"]
    #     req_ministryId = extracted_data["ministryId"]
        
    #     # API endpoint
    #     url = "https://aaf8ece1-3077-4a52-ab05-183a424f6d93-dev.e1-us-east-azure.choreoapis.dev/data-platform/query-api/v1.0/v1/entities/search"
        
    #     # Payload you want to send
    #     payload = {
    #         "id": "",
    #         "kind": {
    #             "major": "Organisation",
    #             "minor": "minister"
    #         },
    #         "name": "",
    #         "created": "",
    #         "terminated": ""
    #     }

    #     # Headers (adjust if the API requires authentication like a token)
    #     headers = {
    #         "Content-Type": "application/json",
    #         # "Authorization": f"Bearer {token}"   # uncomment if required
    #     }

    #     try:
    #         # Send POST request
    #         response = requests.post(url, json=payload, headers=headers)
    #         response.raise_for_status()  # raise error for 4xx/5xx
    #         all_ministries = response.json()
            
    #         for item in all_ministries["body"]:
    #             created_time = item["created"]
    #             if created_time:
    #                 year = created_time[:4]
    #                 if str(year) == str(req_year):
    #                     ministryId = item["id"]
    #                     if str(ministryId) == str(req_ministryId):            
    #                         data_list_for_req_year.append({
    #                         "ministry_id": item["id"],
    #                         "name": item["name"],
    #                         "year": year
    #                     }) 
            
    #         # for item in all_ministries["body"]:
    #         #     created_time = item["created"]
    #         #     if created_time:
    #         #         year = created_time[:4]
    #         #         if str(year) == str(req_year):
    #         #             data_list_for_req_year.append({
    #         #                 "ministry_id": item["id"],
    #         #                 "name": item["name"],
    #         #                 "year": year
    #         #             })            
                    
                
    #         api_output = data_list_for_req_year
            
    #     except Exception as e:
    #         api_output = {"error": str(e)}

    #     return {
    #         "extracted_data": extracted_data,
    #         "api_output": api_output
    #     }
    
    
    
    
    
        