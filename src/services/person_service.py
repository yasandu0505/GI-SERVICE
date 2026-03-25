from src.enums.relationEnum import RelationDirectionEnum
from src.enums.relationEnum import RelationNameEnum
from src.exception.exceptions import BadRequestError
from src.exception.exceptions import NotFoundError
from src.exception.exceptions import InternalServerError
import asyncio
from src.utils.util_functions import Util
from aiohttp import ClientSession
from src.utils import http_client
from src.models.organisation_schemas import Entity, Relation, Kind
from src.enums.kindEnum import KindMajorEnum, KindMinorEnum
from src.models.person_schemas import PersonResponse
from datetime import datetime

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

    def is_president_during(
        self,
        president_relations: list[Relation],
        ministry_relation_start: str,
        ministry_relation_end: str,
    ) -> bool:
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
            if (not pres_end or ministry_relation_start < pres_end) and (
                not ministry_relation_end or ministry_relation_end > pres_start
            ):
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
                relation=Relation(name=RelationNameEnum.AS_APPOINTED.value, direction=RelationDirectionEnum.INCOMING.value),
            )
            president_relations_task = self.opengin_service.fetch_relation(
                entityId=person_id,
                relation=Relation(name=RelationNameEnum.AS_PRESIDENT.value, direction=RelationDirectionEnum.INCOMING.value),
            )

            results_relations = await asyncio.gather(
                ministry_relations_task,
                president_relations_task,
                return_exceptions=True,
            )

            ministry_relations = (
                results_relations[0]
                if not isinstance(results_relations[0], Exception)
                else []
            )
            president_relations = (
                results_relations[1]
                if not isinstance(results_relations[1], Exception)
                else []
            )

            tasks = [
                self.enrich_history_item(relation, president_relations)
                for relation in ministry_relations
                if not relation.endTime  # if this is true, the second condition will not be evaluated
                or relation.startTime.split("T")[0]
                != relation.endTime.split("T")[0]  # short circuit evaluation
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            ministry_history = [
                r for r in results if r and not isinstance(r, Exception)
            ]

            # Sort ministry history:
            # 1.if end_time is None (ongoing), it comes first
            # 2.if multiple end_time is None (ongoing), latest start_time first - descending
            # 3.if end_time is present, sort by end_time descending
            ministry_history.sort(key=Util.history_sort_key, reverse=True)

            # Remove start_time and end_time
            for item in ministry_history:
                item.pop("start_time", None)
                item.pop("end_time", None)

            final_result = {
                "ministry_history": ministry_history,
                "ministries_worked_at": len(ministry_history),
                "worked_as_president": len(president_relations),
            }

            return final_result

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Error fetching person history: {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def enrich_history_item(
        self, relation: Relation, president_relations: list[Relation]
    ):
        try:
            ministry_data = await self.opengin_service.get_entities(
                Entity(id=relation.relatedEntityId)
            )
            is_president = self.is_president_during(
                president_relations, relation.startTime, relation.endTime
            )

            if ministry_data:
                ministry = ministry_data[0]
                name = Util.decode_protobuf_attribute_name(ministry.name)
                term = Util.term(
                    relation.startTime, relation.endTime, get_full_date=True
                )

                return {
                    "id": ministry.id,
                    "name": name,
                    "term": term,
                    "is_president": is_president,
                    "start_time": relation.startTime,
                    "end_time": relation.endTime,
                }
            return None
        except Exception as e:
            logger.error(
                f"Error enriching history item for {relation.relatedEntityId}: {e}"
            )
            return None

    async def fetch_person_profile(self, person_id: str):
        try:
            if not person_id or not person_id.strip():
                raise BadRequestError("Person ID is required")

            person_data_res = await self.opengin_service.get_entities(Entity(id=person_id))

            try:
                encoded_person_profile_data = await self.opengin_service.get_attributes(
                    category_id=person_id,
                    dataset_name=f"{person_id}_profile",
                )
            except (BadRequestError, NotFoundError, InternalServerError) as e:
                logger.info(f"Person profile not available for person {person_id}, falling back to name only. Reason: {type(e).__name__}")
                person_name = Util.decode_protobuf_attribute_name(person_data_res[0].name)
                return PersonResponse(name=person_name)

            formatted_person_profile_data = Util.transform_data_for_chart(
                attribute_data_out={"data": encoded_person_profile_data}
            )
            rows = formatted_person_profile_data["data"]["rows"]

            if not rows:
                raise NotFoundError(f"Profile data not found for person {person_id}")
            
            row = rows[0]

            columns = formatted_person_profile_data["data"]["columns"]

            person_profile_dict = dict(zip(columns, row))

            person_profile_dict["date_of_birth"] = (
                person_profile_dict.get("date_of_birth") or None
            )

            dob_str = person_profile_dict["date_of_birth"]
            if dob_str:
                try:
                    dob = datetime.fromisoformat(dob_str).date()
                except ValueError:
                    dob = None  
            else:
                dob = None

            age = Util.calculate_age(dob) if dob else None
            person_profile_res = PersonResponse(**person_profile_dict, age=age)

            return person_profile_res
            
        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Error fetching person profile: {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def fetch_all_presidents(self):
        """
        Fetches all presidents and their terms.
        
        Returns:
            dict: Dictionary of presidents with their terms and gazettes published by date.
        
        Output Format:
            {
                "president_id": {
                    "id": "president_id",
                    "name": "president_name",
                    "terms": [
                        {
                            "start": "start_date",
                            "end": "end_date",
                            "gazettes_published": [
                                {
                                    "date": "gazette_date",
                                    "ids": ["gazette_id"]
                                }
                            ]
                        }
                    ]
                }
            }
        """
        try:
            president_relations_task = self.opengin_service.fetch_relation(
                entityId="gov_01",
                relation=Relation(name=RelationNameEnum.AS_PRESIDENT.value),
            )

            organization_gazettes_task = self.opengin_service.get_entities(
                Entity(kind=Kind(
                    major=KindMajorEnum.DOCUMENT.value, 
                    minor=KindMinorEnum.EXTRA_ORDINARY_GAZETTE_ORGANISATION.value
                ))
            )
            person_gazettes_task = self.opengin_service.get_entities(
                Entity(kind=Kind(
                    major=KindMajorEnum.DOCUMENT.value, 
                    minor=KindMinorEnum.EXTRA_ORDINARY_GAZETTE_PERSON.value
                ))
            )
            
            results = await asyncio.gather(
                president_relations_task, 
                organization_gazettes_task, 
                person_gazettes_task,
                return_exceptions=True
            )
            president_relations, organization_gazettes, person_gazettes = results
            
            if isinstance(president_relations, Exception):
                logger.error(f"Failed to fetch president relations: {president_relations}")
                raise InternalServerError("An unexpected error occurred while fetching president relations")

            if not president_relations:
                return {}

            # Group relations by id for multiple terms for the same president
            presidents_map = {}
            for relation in president_relations:
                president_id = relation.relatedEntityId
                
                start_date = relation.startTime.split('T')[0]
                end_date = relation.endTime.split('T')[0] if relation.endTime else None
                
                term = {
                    "start": start_date,
                    "end": end_date,
                    "_gazettes_dict": {} # Temporary lookup map for this term
                }
                
                if president_id not in presidents_map:
                    presidents_map[president_id] = {
                        "id": president_id,
                        "terms": []
                    }
                    
                presidents_map[president_id]["terms"].append(term)

            unique_president_ids = list(presidents_map.keys())
            
            # Fetch presidnet details - name
            tasks = [self.opengin_service.get_entities(Entity(id=president_id)) for president_id in unique_president_ids]
            entities_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Update the map with names
            for i, president_id in enumerate(unique_president_ids):
                entity_data = entities_results[i]
                if not isinstance(entity_data, Exception) and entity_data:
                    entity = entity_data[0]
                    decoded_name = Util.decode_protobuf_attribute_name(entity.name)
                    presidents_map[president_id]["name"] = decoded_name

            # Move references for chronological processing
            all_terms = []
            for p_id, p_info in presidents_map.items():
                for term in p_info["terms"]:
                    all_terms.append({
                        "start": term["start"],
                        "end": term.get("end") or "9999-12-31", # far future if ongoing
                        "term_data": term # Reference to the term dictionary
                    })
            
            # Combine all gazettes into a single list
            all_gazettes = []
            if not isinstance(organization_gazettes, Exception) and organization_gazettes:
                all_gazettes.extend(organization_gazettes)
            if not isinstance(person_gazettes, Exception) and person_gazettes:
                all_gazettes.extend(person_gazettes)

            # Sort both lists 
            all_terms.sort(key=lambda x: x["start"])

            all_gazettes = [g for g in all_gazettes if g.created]
            all_gazettes.sort(key=lambda x: x.created)

            # gazette grouping by term
            # currently alive term (so that previous terms are not considered)
            term_index = 0
            n_terms = len(all_terms)

            for gazette in all_gazettes:
                gazette_date = gazette.created.split("T")[0]
                
                # Skip terms that ended on or before this gazette was published 
                while term_index < n_terms and all_terms[term_index]["end"] <= gazette_date:
                    term_index += 1
                
                if term_index < n_terms:
                    term_item = all_terms[term_index]
                    
                    try:
                        gazette_id = Util.decode_protobuf_attribute_name(gazette.name)
                    except Exception as e:
                        logger.warning(f"Could not decode gazette name, falling back to raw string: {e}")
                        gazette_id = str(gazette.name)

                    term_dict = term_item["term_data"]
                    date_dict = term_dict["_gazettes_dict"]
                    if gazette_date not in date_dict:
                        date_dict[gazette_date] = []
                    
                    if gazette_id not in date_dict[gazette_date]:
                        date_dict[gazette_date].append(gazette_id)

            # Convert intermediate _gazettes_dict inside each term to the final format
            for president_info in presidents_map.values():
                for term in president_info["terms"]:
                    date_dict = term.pop("_gazettes_dict")
                    
                    gazettes_list = [{"date": k, "ids": v} for k, v in date_dict.items()]
                    gazettes_list.sort(key=lambda x: x["date"])
                    
                    term["gazettes_published"] = gazettes_list

             # Sort the presidents by their latest term's start date in descending order
            def get_latest_start(item):
                president_data = item[1]
                return max(term["start"] for term in president_data["terms"])

            sorted_presidents_map = dict(
                sorted(presidents_map.items(), key=get_latest_start, reverse=True)
            )

            return sorted_presidents_map
        except Exception as e:
            logger.error(f"Error fetching all presidents: {e}")
            raise InternalServerError("An unexpected error occurred") from e
