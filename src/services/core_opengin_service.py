from src.exception.exceptions import GatewayTimeoutError
from aiohttp.client_exceptions import ClientResponseError
from src.models.organisation_v1_schemas import Entity
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
        
    async def get_entity_by_id(self,entity: Entity):

        if not entity:
            raise BadRequestError("Entity is required")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/search"
        payload = entity
        headers = {"Content-Type":"application/json"}      

        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                
                if response.status == 404:
                    raise NotFoundError(f"Core Service Error: Entity not found for id {entity.id}")
                
                response.raise_for_status()
                res_json = await response.json()
                response_list = res_json.get("body", [])

                if response_list is None:
                    raise NotFoundError(f"Core Service Error: Entity not found for id {entity.id}")

                return response_list[0]    
                
        except NotFoundError:
            raise       
        except ClientResponseError as e:
            if e.status == 400:
                raise BadRequestError(f"Core Service Error: {str(e)}")
            elif e.status == 500:
                raise InternalServerError(f"Core Service Error: {str(e)}")
            elif e.status == 503:
                raise ServiceUnavailableError(f"Core Service Error: {str(e)}")
            elif e.status == 504:
                raise GatewayTimeoutError(f"Core Service Error: {str(e)}")
            else:
                raise InternalServerError(f"Core Service Error: {str(e)}")
        except ClientError as e:
            raise ServiceUnavailableError(f"Core Service Error: {str(e)}")
        except Exception as e:
            raise InternalServerError(f"Core Service Error: {str(e)}")
    
    async def fetch_relation(self, entityId, relationName="", activeAt="", relatedEntityId="", startTime="", endTime="", id="", direction="OUTGOING"):
        
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
            "endTime": endTime,
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

        except NotFoundError:
            raise       
        except ClientResponseError as e:
            if e.status == 400:
                raise BadRequestError(f"Core Service Error: {str(e)}")
            elif e.status == 500:
                raise InternalServerError(f"Core Service Error: {str(e)}")
            elif e.status == 503:
                raise ServiceUnavailableError(f"Core Service Error: {str(e)}")
            elif e.status == 504:
                raise GatewayTimeoutError(f"Core Service Error: {str(e)}")
            else:
                raise InternalServerError(f"Core Service Error: {str(e)}")
        except ClientError as e:
            print(f'Core Service Error: Failed to fetch relation data for entity {validated_id} due to a network error: {str(e)}')
            raise ServiceUnavailableError(f'Core Service Error: {str(e)}')
        except Exception as e:
            print(f'Core Service Error: {str(e)}')
            raise InternalServerError(f'Core Service Error: {str(e)}')