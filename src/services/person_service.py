
from src.exception.exceptions import BadRequestError
from src.exception.exceptions import NotFoundError
from src.exception.exceptions import InternalServerError
import asyncio
from src.utils.util_functions import Util
from aiohttp import ClientSession
from src.utils import http_client
from src.models.organisation_schemas import Entity, Relation
import logging

logger = logging.getLogger(__name__)

class PersonService:
    """
    This service is responsible for executing aggregate functions by calling the OpenGINService and processing the returned data.
    """
    def __init__(self, config: dict, opengin_service):
        self.config = config
        self.opengin_service = opengin_service

    @property
    def session(self) -> ClientSession:
        """Access the global session"""
        return http_client.session  
    
    async def is_president_during(self, person_id: str, ministry_relation_start: str, ministry_relation_end: str) -> bool:
        """
        Check if the person is president during the given ministry term
        
        :param person_id: Person Id
        :param ministry_relation_start: Ministry relation start time
        :param ministry_relation_end: Ministry relation end time

        return type: bool
        """
        relation = Relation(name="AS_PRESIDENT", direction="INCOMING")
        president_relations = await self.opengin_service.fetch_relation(
            entityId=person_id,
            relation=relation
        )
        if not president_relations:
            return False

        for r in president_relations:
            pres_start = r.startTime
            pres_end = r.endTime
            if (not pres_end or ministry_relation_start <= pres_end) and (not ministry_relation_end or ministry_relation_end >= pres_start):
                return True
        
        return False

    async def fetch_person_history(self, person_id: str):
        """
        Fetch person history by person id
        
        :param person_id: Person Id

        output format: 
        {
            "body": {
               "ministry_history": [
                    {
                        "id": "",
                        "name": "",
                        "term": "",
                        "is_president": ""
                    }
                ]
                "ministries_worked_at": "",
                "worked_as_president": ""
            }
        }
        """
        try:
            if not person_id or not person_id.strip():
                raise BadRequestError("Person ID is required")

            ministry_relations_task = self.opengin_service.fetch_relation(
                entityId=person_id,
                relation=Relation(name="AS_APPOINTED", direction="INCOMING")
            )
            president_relations_task = self.opengin_service.fetch_relation(
                entityId=person_id,
                relation=Relation(name="AS_PRESIDENT", direction="INCOMING")
            )

            results_relations = await asyncio.gather(ministry_relations_task, president_relations_task, return_exceptions=True)
            
            ministry_relations = results_relations[0] if not isinstance(results_relations[0], Exception) else []
            president_relations = results_relations[1] if not isinstance(results_relations[1], Exception) else []

            tasks = [
                self.enrich_history_item(person_id, rel)
                for rel in ministry_relations
                if rel.startTime and (
                    not rel.endTime or #if this is true, the second condition will not be evaluated
                    rel.startTime.split("T")[0] != rel.endTime.split("T")[0] #short circuit evaluation
                ) 
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            ministry_history = [
                r for r in results if r and not isinstance(r, Exception)
            ]

            final_result = {
                "body": {
                    "ministry_history": ministry_history,
                    "ministries_worked_at": len(ministry_history),
                    "worked_as_president": len(president_relations)
                }
            }

            return final_result

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f'Error fetching person history: {e}')
            raise InternalServerError("An unexpected error occurred") from e

    async def enrich_history_item(self, person_id: str, relation: Relation):
        try:
            ministry_task = self.opengin_service.get_entities(Entity(id=relation.relatedEntityId))
            is_president_task = self.is_president_during(person_id, relation.startTime, relation.endTime)
            
            ministry_data, is_president = await asyncio.gather(ministry_task, is_president_task)

            if ministry_data:
                ministry = ministry_data[0]
                name = Util.decode_protobuf_attribute_name(ministry.name)
                term = Util.term(relation.startTime, relation.endTime, show_full_date=True)
                
                return {
                    "id": ministry.id,
                    "name": name,
                    "term": term,
                    "is_president": is_president
                }
            return None
        except Exception as e:
            logger.error(f"Error enriching history item for {relation.relatedEntityId}: {e}")
            return None
