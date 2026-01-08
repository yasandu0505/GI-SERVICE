from src.models.organisation_schemas import Entity, Relation
from src.exception.exceptions import BadRequestError
from src.exception.exceptions import InternalServerError
from src.exception.exceptions import NotFoundError
from aiohttp import ClientSession
from src.utils.http_client import http_client
from src.core.config import settings
import logging

logger = logging.getLogger(__name__)

class OpenGINService:
    """
    The OpenGINService directly interfaces with the OpenGIN APIs to retrieve data.
    """
    def __init__(self, config: dict):
        self.config = config

    @property
    def session(self) -> ClientSession:
        return http_client.session
        
    async def get_entity(self,entity: Entity):

        if not entity:
            raise BadRequestError("Entity is required")

        url = f"{settings.BASE_URL_QUERY}/v1/entities/search"
        headers = {"Content-Type":"application/json"}      
        payload = entity.model_dump()

        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                
                if response.status == 404:
                    raise NotFoundError(f"Read API Error: Entity not found for id {entity.id}")
                
                response.raise_for_status()
                res_json = await response.json()
                response_list = res_json.get("body", [])

                if not response_list:
                    raise NotFoundError(f"Read API Error: Entity not found for id {entity.id}")

                result = Entity.model_validate(response_list[0])
                return result    
                
        except NotFoundError:
            raise       
        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f'Read API Error: {str(e)}')
            raise InternalServerError("An unexpected error occurred") from e
    
    async def fetch_relation(self, entityId: str, relation: Relation):
        
        if not entityId or not relation:
            raise BadRequestError("Entity ID is required")
        
        stripped_entity_id = str(entityId).strip()
        if not stripped_entity_id:
            raise BadRequestError("Entity ID can not be empty")
        
        url = f"{settings.BASE_URL_QUERY}/v1/entities/{stripped_entity_id}/relations"
        headers = {"Content-Type": "application/json"}  
        payload = relation.model_dump()

        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                result = [Relation.model_validate(item) for item in data]
                return result

        except NotFoundError:
            raise    
        except BadRequestError:
            raise   
        except Exception as e:
            logger.error(f'Read API Error: {str(e)}')
            raise InternalServerError("An unexpected error occurred") from e