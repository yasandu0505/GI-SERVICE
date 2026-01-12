from src.models.organisation_schemas import Entity, Relation
from src.exception.exceptions import BadRequestError
from src.exception.exceptions import InternalServerError
from src.exception.exceptions import NotFoundError
from google.api_core import retry_async
from google.api_core import exceptions
from aiohttp import ClientSession
from src.utils.http_client import http_client
from src.core.config import settings
import logging

logger = logging.getLogger(__name__)

def should_retry(exception):
    """
    Determine if the request should be retried based on the exception type.
    Returns False for NotFoundError and BadRequestError to skip retries.
    """
    if isinstance(exception, (NotFoundError, BadRequestError)):
        return False
    return True

api_retry_decorator = retry_async.AsyncRetry(
    predicate=should_retry,  # custom predicate to skip NotFoundError and BadRequestError
    initial=1.0,  # initial delay in seconds
    maximum=5.0, # maximum delay in seconds
    multiplier=2.0, # delay multiplier
)

class OpenGINService:
    """
    The OpenGINService directly interfaces with the OpenGIN APIs to retrieve data.
    """
    def __init__(self, config: dict):
        self.config = config

    @property
    def session(self) -> ClientSession:
        return http_client.session
    
    @api_retry_decorator
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
                if response.status == 400:
                    raise BadRequestError(f"Read API Error: Bad request for id {entity.id}")
                
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
            raise InternalServerError(f"Read API Error: {str(e)}")
    
    @api_retry_decorator
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
                if response.status == 404:
                    raise NotFoundError(f"Read API Error: Relation not found for id {entityId}")
                if response.status == 400:
                    raise BadRequestError(f"Read API Error: Bad request for id {entityId}")
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
            raise InternalServerError(f'Read API Error: {str(e)}')