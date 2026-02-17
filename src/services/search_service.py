import asyncio
import logging
from typing import List, Dict, Any, Optional
from src.exception.exceptions import BadRequestError, InternalServerError
from src.models.organisation_schemas import Entity, Kind
from src.models.search_schemas import SearchResult, SearchResponse
from src.utils.util_functions import Util

logger = logging.getLogger(__name__)

# Valid entity types for filtering
VALID_ENTITY_TYPES = {"department", "stateMinister", "cabinetMinister", "dataset", "person"}
class SearchService:
    """
    This service handles unified search across departments, ministers, datasets, and persons.
    Uses OpenGINService for data retrieval and returns aggregated, scored results.
    """

    def __init__(self, config: dict, opengin_service):
        self.config = config
        self.opengin_service = opengin_service

    # Unified search
    async def unified_search(self, query: str, as_of_date: str, limit: Optional[int] = None, entity_types: Optional[List[str]] = None) -> SearchResponse:
        """
        Main entry point for unified search across all entity types.

        Searches departments, ministers, datasets, and persons in parallel,
        then merges, scores, sorts, and limits the results.

        Args:
            query: Search query string (min 2 characters)
            as_of_date: Date for time-sensitive search (YYYY-MM-DD)
            limit: Maximum number of results to return
            entity_types: Optional list of entity types to filter (department, stateMinister, cabinetMinister, dataset, person)

        Returns:
            SearchResponse with merged results from all entity types
        """
        if not query or len(query.strip()) < 2:
            raise BadRequestError("Search query must be at least 2 characters")

        if not as_of_date:
            raise BadRequestError("Search date is required")

        query = query.strip()

        # Validate and normalize entity types filter
        types_to_search = self._get_types_to_search(entity_types)

        try:
            # Entity type configuration: maps entity type to (major, minor, display_name)
            entity_config = {
                "department": ("Organisation", "department", "departments"),
                "stateMinister": ("Organisation", "stateMinister", "stateMinisters"),
                "cabinetMinister": ("Organisation", "cabinetMinister", "cabinetMinisters"),
                "dataset": ("Dataset", "tabular", "datasets"),
                "person": ("Person", "citizen", "persons"),
            }

            # Build search tasks dynamically based on requested types
            search_tasks = []
            search_type_names = []

            for entity_type in types_to_search:
                if entity_type in entity_config:
                    major, minor, display_name = entity_config[entity_type]
                    search_tasks.append(self.entity_specific_search(major, minor, query, as_of_date, limit))
                    search_type_names.append(display_name)

            # Run selected searches in parallel
            results = await asyncio.gather(*search_tasks, return_exceptions=True)

            # Collect successful results, log errors
            all_results: List[Dict[str, Any]] = []

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error searching {search_type_names[i]}: {result}")
                else:
                    all_results.extend(result)

            # Sort by match score (highest first)
            all_results.sort(key=lambda x: x.get("match_score", 0), reverse=True)

            # Limit results if limit is specified
            limited_results = all_results[:limit] if limit else all_results

            # Convert to SearchResult models
            search_results = [SearchResult(**r) for r in limited_results]

            return SearchResponse(
                query=query,
                as_of_date=as_of_date,
                total=len(search_results),
                results=search_results
            )

        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Unified search failed: {e}")
            raise InternalServerError("An unexpected error occurred") from e

    # Entity specific search
    async def entity_specific_search(self, major: str, minor: str, query: str, as_of_date: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Generic search function that supports departments, ministers, datasets, and persons.
        
        Determines the search type based on major/minor parameters and executes
        the appropriate search logic with type-specific result formatting.

        Args:
            major: Major kind (e.g., "Organisation", "Dataset", "Person")
            minor: Minor kind (e.g., "department", "stateMinister", "cabinetMinister", "tabular", "citizen")
            query: Search query string
            as_of_date: Date for filtering (YYYY-MM-DD)
            limit: Maximum results to return

        Returns:
            List of result dictionaries with appropriate type-specific fields
        """

        try:
            entity = Entity(name=query,kind=Kind(major=major, minor=minor))
            all_entities = await self.opengin_service.get_entities(entity=entity)

            search_year = Util.extract_year(as_of_date)
            matching: List[Dict[str, Any]] = []

            # Determine entity type based on major/minor combination
            entity_type = self._determine_entity_type(major, minor)

            # Process each entity based on its type
            for item in all_entities:
                name = Util.decode_protobuf_attribute_name(item.name)

                # Extract year from created field
                item_year = Util.extract_year(item.created) if item.created else 9999

                if item_year > search_year:
                    continue

                if entity_type != "unknown":
                    display_name = name
                    if entity_type == "dataset":
                        # Remove year suffix from name for display
                        display_name = Util.get_name_without_year(name)
                        display_name = Util.to_title_case(display_name)

                    matching.append({
                        "type": entity_type,
                        "id": item.id,
                        "name": display_name,
                        "created": item.created,
                        "terminated": item.terminated if item.terminated else "",
                        "match_score": Util.calculate_match_score(query, name)
                    })

            # Sort by match score (highest first)
            matching.sort(key=lambda x: x.get("match_score", 0), reverse=True)

            # Apply limit if specified
            return matching[:limit] if limit else matching

        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Error in generic_search for {major}/{minor}: {e}")
            return []

    # Helper methods
    def _get_types_to_search(self, entity_types: Optional[List[str]]) -> set:
        """
        Validate and normalize entity types filter.

        Args:
            entity_types: Optional list of entity types from request

        Returns:
            Set of valid entity types to search. Returns all types if none specified.
        """
        if not entity_types:
            return VALID_ENTITY_TYPES.copy()

        # Normalize to lowercase and filter valid types
        requested_types = {t.lower().strip() for t in entity_types if t}
        valid_requested = requested_types & VALID_ENTITY_TYPES

        if not valid_requested:
            # If all requested types are invalid, return all valid types
            logger.warning(f"Invalid entity types requested: {entity_types}. Searching all types.")
            return VALID_ENTITY_TYPES.copy()

        return valid_requested

    def _determine_entity_type(self, major: str, minor: str) -> str:
        """
        Determine the entity type based on major/minor kind combination.

        Args:
            major: Major kind (e.g., "Organisation", "Dataset", "Person")
            minor: Minor kind (e.g., "department", "stateMinister", "cabinetMinister", "tabular", "citizen")

        Returns:
            Entity type string: "department", "stateMinister", "cabinetMinister", "dataset", or "person"
        """
        # Map major/minor combinations to entity types
        type_mapping = {
            ("Organisation".lower(), "department".lower()): "department",
            ("Organisation".lower(), "stateMinister".lower()): "stateMinister",
            ("Organisation".lower(), "cabinetMinister".lower()): "cabinetMinister",
            ("Dataset".lower(), "tabular".lower()): "dataset",
            ("Person".lower(), "citizen".lower()): "person",
        }

        key = (major.lower(), minor.lower())
        entity_type = type_mapping.get(key, "unknown")

        if entity_type == "unknown":
            logger.warning(f"Unknown entity type for major={major}, minor={minor}")

        return entity_type


