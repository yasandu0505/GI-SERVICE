from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from datetime import datetime
from src.dependencies import get_config
from src.services.search_service import SearchService
from src.services.opengin_service import OpenGINService
from src.models.search_schemas import SearchResponse

router = APIRouter(prefix="/v1/search", tags=["Search"])


def get_search_service(config: dict = Depends(get_config)):
    """Factory function to create SearchService with dependencies."""
    opengin_service = OpenGINService(config=config)
    return SearchService(config, opengin_service)


@router.get(
    "",
    response_model=SearchResponse,
    summary="Unified search across all entities.",
    description="Search departments, ministers, datasets, and persons with time-sensitivity support. Returns mixed results sorted by relevance."
)
async def search(
    q: str = Query(
        ...,
        min_length=2,
        description="Search query (minimum 2 characters)"
    ),
    as_of_date: Optional[str] = Query(
        None,
        description="Date for historical search (YYYY-MM-DD). Defaults to today.",
        pattern=r"^\d{4}-\d{2}-\d{2}$"
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description="Maximum number of results to return. If not specified, returns all results."
    ),
    entity_types: Optional[List[str]] = Query(
        None,
        alias="type",
        description="Filter by entity type(s): department, minister, dataset, person. Can specify multiple."
    ),
    service: SearchService = Depends(get_search_service)
):
    """
    Unified search endpoint for departments, ministers, datasets, and persons.

    Supports time-sensitive queries to search historical data.

    Examples:
        - Current search: GET /v1/search?q=health
        - Historical search: GET /v1/search?q=health&as_of_date=2020-03-15
        - Limited results: GET /v1/search?q=education&limit=10
        - Filter by type: GET /v1/search?q=expenditure&type=dataset
        - Multiple types: GET /v1/search?q=health&type=department&type=person

    Args:
        q: Search query (minimum 2 characters)
        as_of_date: Optional date for historical search (YYYY-MM-DD)
        limit: Maximum number of results. If not specified, all results are returned.
        entity_types: Optional filter by entity type(s)

    Returns:
        SearchResponse with mixed results from all entity types
    """
    # Default to today if no date provided
    search_date = as_of_date or datetime.now().strftime("%Y-%m-%d")

    result = await service.unified_search(
        query=q,
        as_of_date=search_date,
        limit=limit,
        entity_types=entity_types
    )

    return result
