

class OpenGINService:
    def __init__(self, config: dict):
        self.config = config
        
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
    
    async def fetch_relation(self,session, id, relationName, activeAt, direction="OUTGOING"):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{id}/relations"
        headers = {"Content-Type": "application/json"}  
        payload = {
            "relatedEntityId": "",
            "startTime": "",
            "endTime": "",
            "id": "",
            "name": relationName,
            "activeAt": activeAt,
            "direction": direction,
        }
        async with session.post(url, json=payload, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data