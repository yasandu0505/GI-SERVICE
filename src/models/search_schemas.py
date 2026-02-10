from pydantic import BaseModel
from typing import List

class SearchResult(BaseModel):
    """
    Individual search result item.

    Represents a single entity from the search results, which can be
    a department, minister, dataset, or person.
    
    All entity types now share a consistent structure with:
    - type: Entity type identifier
    - id: Unique entity identifier
    - name: Entity name (cleaned and formatted for datasets)
    - created: Creation/start date (ISO format or empty string)
    - terminated: Termination/end date (ISO format or empty string if still active)
    - match_score: Relevance score (0.0 to 1.0)
    """
    type: str  # "department", "minister", "dataset", or "person"
    id: str
    name: str
    created: str  
    terminated: str  
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
