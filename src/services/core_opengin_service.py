from aiohttp import ClientSession
from src.utils.http_client import http_client

class OpenGINService:
    def __init__(self, config: dict):
        self.config = config

    @property
    def session(self) -> ClientSession:
        return http_client.session
        
    async def get_node_data_by_id(self,entityId):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/search"
        payload = {
            "id": entityId
        }
        headers = {"Content-Type":"application/json"}      

        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                res_json = await response.json()
                response_list = res_json.get("body",[])
                return response_list[0]                        
        except Exception as e:
            return {"error": f"Failed to fetch entity data by id {entityId}: {str(e)}"}  
    
    async def fetch_relation(self, entityId, relationName, activeAt, relatedEntityId="", startTime="", endTIme="", id="", direction="OUTGOING"):
        url = f"{self.config['BASE_URL_QUERY']}/v1/entities/{entityId}/relations"
        headers = {"Content-Type": "application/json"}  
        payload = {
            "relatedEntityId": relatedEntityId,
            "startTime": startTime,
            "endTime": endTIme,
            "id": id,
            "name": relationName,
            "activeAt": activeAt,
            "direction": direction,
        }
        async with self.session.post(url, json=payload, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data