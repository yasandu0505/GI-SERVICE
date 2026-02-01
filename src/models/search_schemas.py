from pydantic import BaseModel
from typing import Optional, List


class SearchResult(BaseModel):
    """
    Individual search result item.

    Represents a single entity from the search results, which can be
    a department, minister, dataset, or person.
    """
    type: str  # "department", "minister", "dataset", or "person"
    id: str
    name: Optional[str] = None

    # Department-specific fields
    parent_portfolio: Optional[str] = None
    active_from: Optional[str] = None
    active_to: Optional[str] = None

    # Minister-specific fields
    portfolio: Optional[str] = None
    term_start: Optional[str] = None
    term_end: Optional[str] = None
    is_president: Optional[bool] = None

    # Dataset-specific fields
    source: Optional[str] = None
    year: Optional[str] = None

    # Person-specific fields
    created: Optional[str] = None

    # Relevance score
    match_score: float = 0.0


class SearchResponse(BaseModel):
    """
    Search API response.

    Contains the search query, date context, and list of mixed results
    from all entity types sorted by relevance.
    """
    query: str
    as_of_date: str
    total: int
    results: List[SearchResult]
