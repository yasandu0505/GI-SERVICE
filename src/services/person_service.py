
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
    
    def is_president_during(self, president_relations: list[Relation], ministry_relation_start: str, ministry_relation_end: str) -> bool:
        """
        Check if the person is president during the given ministry term
        
        :param president_relations: List of president relations
        :param ministry_relation_start: Ministry relation start time
        :param ministry_relation_end: Ministry relation end time

        return type: bool
        """
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
                self.enrich_history_item(relation, president_relations)
                for relation in ministry_relations
                if not relation.endTime or #if this is true, the second condition will not be evaluated
                   relation.startTime.split("T")[0] != relation.endTime.split("T")[0] #short circuit evaluation
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            ministry_history = [
                r for r in results if r and not isinstance(r, Exception)
            ]

            #Sort ministry history:
            #1.if end_time is None (ongoing), it comes first
            #2.if multiple end_time is None (ongoing), latest start_time first - descending
            #3.if end_time is present, sort by end_time descending
            def sort_key(item):
                end = item.get("end_time")
                start = item.get("start_time")
                
                effective_end = end if end else "9999-12-31" 
                
                return (effective_end, start)

            ministry_history.sort(key=sort_key, reverse=True)

            # Remove start_time and end_time
            for item in ministry_history:
                item.pop("start_time", None)
                item.pop("end_time", None)

            final_result = {
                "ministry_history": ministry_history,
                "ministries_worked_at": len(ministry_history),
                "worked_as_president": len(president_relations)
            }

            return final_result

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f'Error fetching person history: {e}')
            raise InternalServerError("An unexpected error occurred") from e

    async def enrich_history_item(self, relation: Relation, president_relations: list[Relation]):
        try:
            ministry_data = await self.opengin_service.get_entities(Entity(id=relation.relatedEntityId))
            is_president = self.is_president_during(president_relations, relation.startTime, relation.endTime)

            if ministry_data:
                ministry = ministry_data[0]
                name = Util.decode_protobuf_attribute_name(ministry.name)
                term = Util.term(relation.startTime, relation.endTime, get_full_date=True)
                
                return {
                    "id": ministry.id,
                    "name": name,
                    "term": term,
                    "is_president": is_president,
                    "start_time": relation.startTime,
                    "end_time": relation.endTime
                }
            return None
        except Exception as e:
            logger.error(f"Error enriching history item for {relation.relatedEntityId}: {e}")
            return None
