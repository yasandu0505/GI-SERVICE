from src.exception.exceptions import BadRequestError
from src.exception.exceptions import InternalServerError
from src.exception.exceptions import ServiceUnavailableError
from src.exception.exceptions import NotFoundError
from aiohttp import ClientSession, ClientError
from src.utils.http_client import http_client
from src.core.config import settings

class OpenGINService:
    """
    The OpenGINService directly interfaces with the OpenGIN APIs to retrieve data.
    """
    def __init__(self, config: dict):
        self.config = config

    @property
    def session(self) -> ClientSession:
        return http_client.session
        
    async def get_entity_by_id(self,entityId):

        if not entityId:
            raise BadRequestError("Entity ID is required")
        
        validated_id = str(entityId).strip()
        if not validated_id:
            raise BadRequestError("Entity ID can not be empty")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/search"
        payload = {
            "id": validated_id
        }
        headers = {"Content-Type":"application/json"}      

        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                res_json = await response.json()
                response_list = res_json.get("body", None)
                return response_list[0]                        
        except ClientError as e:
            raise ServiceUnavailableError(f"Failed to fetch entity data by id {validated_id} due to a network error: {str(e)}")
        except Exception as e:
            raise InternalServerError(str(e))
    
    async def fetch_relation(self, entityId, relationName="", activeAt="", relatedEntityId="", startTime="", endTIme="", id="", direction="OUTGOING"):
        
        if not entityId:
            raise BadRequestError("Entity ID is required")
        
        validated_id = str(entityId).strip()
        if not validated_id:
            raise BadRequestError("Entity ID can not be empty")
        
        url = f"{settings.BASE_URL_QUERY}/v1/entities/{validated_id}/relations"
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
        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data
        except ClientError as e:
            raise ServiceUnavailableError(f"Failed to fetch relation data for entity {validated_id} due to a network error: {str(e)}")
        except Exception as e:
            raise InternalServerError(str(e))