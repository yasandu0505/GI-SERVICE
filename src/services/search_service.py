import asyncio
import logging
from typing import List, Dict, Any, Optional
from src.exception.exceptions import BadRequestError, InternalServerError
from src.models.organisation_schemas import Entity, Kind
from src.models.search_schemas import SearchResult, SearchResponse
from src.utils.util_functions import Util

logger = logging.getLogger(__name__)

# Valid entity types for filtering
VALID_ENTITY_TYPES = {"department", "minister", "dataset", "person"}


class SearchService:
    """
    This service handles unified search across departments, ministers, datasets, and persons.
    Uses OpenGINService for data retrieval and returns aggregated, scored results.
    """

    def __init__(self, config: dict, opengin_service):
        self.config = config
        self.opengin_service = opengin_service

    # ============ MAIN SEARCH METHOD ============

    async def unified_search(self, query: str, as_of_date: str, limit: int = 20, entity_types: Optional[List[str]] = None) -> SearchResponse:
        """
        Main entry point for unified search across all entity types.

        Searches departments, ministers, datasets, and persons in parallel,
        then merges, scores, sorts, and limits the results.

        Args:
            query: Search query string (min 2 characters)
            as_of_date: Date for time-sensitive search (YYYY-MM-DD)
            limit: Maximum number of results to return
            entity_types: Optional list of entity types to filter (department, minister, dataset, person)

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
            # Build search tasks based on requested types
            search_tasks = []
            search_type_names = []

            if "department" in types_to_search:
                search_tasks.append(self.search_departments(query, as_of_date, limit))
                search_type_names.append("departments")

            if "minister" in types_to_search:
                search_tasks.append(self.search_ministers(query, as_of_date, limit))
                search_type_names.append("ministers")

            if "dataset" in types_to_search:
                search_tasks.append(self.search_datasets(query, as_of_date, limit))
                search_type_names.append("datasets")

            if "person" in types_to_search:
                search_tasks.append(self.search_persons(query, as_of_date, limit))
                search_type_names.append("persons")

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

            # Limit results
            limited_results = all_results[:limit]

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

    # ============ ENTITY-SPECIFIC SEARCH METHODS ============

    async def search_departments(self, query: str, as_of_date: str, limit: int) -> List[Dict[str, Any]]:
        """
        Search departments active on the given date.

        Kind: major="Organisation", minor="department"
        Time-sensitive: Filters entities that existed by the search date.

        Args:
            query: Search query string
            as_of_date: Date for filtering (YYYY-MM-DD)
            limit: Maximum results to return

        Returns:
            List of department result dictionaries
        """
        try:
            entity = Entity(kind=Kind(major="Organisation", minor="department"))
            all_departments = await self.opengin_service.get_entities(entity=entity)

            search_year = self._extract_year(as_of_date)
            matching: List[Dict[str, Any]] = []

            for dept in all_departments:
                name = Util.decode_protobuf_attribute_name(dept.name)

                # Check if entity existed by the search date
                dept_year = self._extract_year(dept.created) if dept.created else 0

                if self._matches_query(query, name) and dept_year <= search_year:
                    matching.append({
                        "type": "department",
                        "id": dept.id,
                        "name": name,
                        "active_from": dept.created,
                        "active_to": dept.terminated if dept.terminated else None,
                        "match_score": self._calculate_match_score(query, name)
                    })

            return matching[:limit]

        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Error searching departments: {e}")
            return []

    async def search_ministers(self, query: str, as_of_date: str, limit: int) -> List[Dict[str, Any]]:
        """
        Search ministers (portfolios) active on the given date.

        Kind: major="Organisation", minor="minister"
        Time-sensitive: Filters entities that existed by the search date.

        Args:
            query: Search query string
            as_of_date: Date for filtering (YYYY-MM-DD)
            limit: Maximum results to return

        Returns:
            List of minister result dictionaries
        """
        try:
            entity = Entity(kind=Kind(major="Organisation", minor="minister"))
            all_ministers = await self.opengin_service.get_entities(entity=entity)

            search_year = self._extract_year(as_of_date)
            matching: List[Dict[str, Any]] = []

            for minister in all_ministers:
                name = Util.decode_protobuf_attribute_name(minister.name)

                # Check if entity existed by the search date
                minister_year = self._extract_year(minister.created) if minister.created else 0

                if self._matches_query(query, name) and minister_year <= search_year:
                    matching.append({
                        "type": "minister",
                        "id": minister.id,
                        "name": name,
                        "term_start": minister.created,
                        "term_end": minister.terminated if minister.terminated else None,
                        "match_score": self._calculate_match_score(query, name)
                    })

            return matching[:limit]

        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Error searching ministers: {e}")
            return []

    async def search_datasets(self, query: str, as_of_date: str, limit: int) -> List[Dict[str, Any]]:
        """
        Search datasets by title, filtered by year.

        Kind: major="Dataset", minor="tabular"
        Time-sensitive: Filters datasets with created year <= search year.

        Args:
            query: Search query string
            as_of_date: Date for filtering (YYYY-MM-DD)
            limit: Maximum results to return

        Returns:
            List of dataset result dictionaries
        """
        try:
            entity = Entity(kind=Kind(major="Dataset", minor="tabular"))
            all_datasets = await self.opengin_service.get_entities(entity=entity)

            search_year = self._extract_year(as_of_date)
            matching: List[Dict[str, Any]] = []

            for dataset in all_datasets:
                name = Util.decode_protobuf_attribute_name(dataset.name)

                # Check year from created field
                dataset_year = self._extract_year(dataset.created) if dataset.created else 9999

                if self._matches_query(query, name) and dataset_year <= search_year:
                    # Remove year suffix from name for display
                    display_name = Util.get_name_without_year(name)
                    display_name = Util.to_title_case(display_name)

                    matching.append({
                        "type": "dataset",
                        "id": dataset.id,
                        "name": display_name,
                        "year": str(dataset_year) if dataset_year != 9999 else None,
                        "match_score": self._calculate_match_score(query, name)
                    })

            return matching[:limit]

        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Error searching datasets: {e}")
            return []

    async def search_persons(self, query: str, as_of_date: str, limit: int) -> List[Dict[str, Any]]:
        """
        Search persons (citizens) by name, filtered by year.

        Kind: major="Person", minor="citizen"
        Time-sensitive: Filters persons with created year <= search year.

        Args:
            query: Search query string
            as_of_date: Date for filtering (YYYY-MM-DD)
            limit: Maximum results to return

        Returns:
            List of person result dictionaries
        """
        try:
            entity = Entity(kind=Kind(major="Person", minor="citizen"))
            all_persons = await self.opengin_service.get_entities(entity=entity)

            search_year = self._extract_year(as_of_date)
            matching: List[Dict[str, Any]] = []

            for person in all_persons:
                name = Util.decode_protobuf_attribute_name(person.name)

                # Check year from created field
                person_year = self._extract_year(person.created) if person.created else 9999

                if self._matches_query(query, name) and person_year <= search_year:
                    matching.append({
                        "type": "person",
                        "id": person.id,
                        "name": name,
                        "created": person.created,
                        "match_score": self._calculate_match_score(query, name)
                    })

            return matching[:limit]

        except BadRequestError:
            raise
        except Exception as e:
            logger.error(f"Error searching persons: {e}")
            return []

    # ============ HELPER METHODS ============

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

    def _calculate_match_score(self, query: str, text: str) -> float:
        """
        Calculate relevance score for search match.

        Scoring:
            - Exact match: 1.0
            - Starts with query: 0.8
            - Contains query: 0.6
            - No match: 0.0

        Args:
            query: Search query string
            text: Text to match against

        Returns:
            Float score between 0.0 and 1.0
        """
        if not text:
            return 0.0

        query_lower = query.lower().strip()
        text_lower = text.lower().strip()

        if text_lower == query_lower:
            return 1.0
        elif text_lower.startswith(query_lower):
            return 0.8
        elif query_lower in text_lower:
            return 0.6
        else:
            return 0.0

    def _matches_query(self, query: str, text: str) -> bool:
        """
        Check if text contains query (case-insensitive).

        Args:
            query: Search query string
            text: Text to check

        Returns:
            True if text contains query, False otherwise
        """
        if not text or not query:
            return False

        return query.lower().strip() in text.lower().strip()

    def _extract_year(self, date_string: str) -> int:
        """
        Extract year from a date string.

        Handles formats:
            - YYYY-MM-DD
            - YYYY-MM-DDTHH:MM:SSZ

        Args:
            date_string: Date string to parse

        Returns:
            Year as integer, or 0 if parsing fails
        """
        if not date_string:
            return 0

        try:
            return int(date_string.split("-")[0])
        except (ValueError, IndexError):
            return 0
