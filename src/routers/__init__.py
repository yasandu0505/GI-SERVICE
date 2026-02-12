from .data_router import router as data_router
from .organisation_router import router as organisation_router
from .search_router import router as search_router
from .person_router import router as person_router

__all__ = [
    "data_router",
    "organisation_router",
    "search_router",
    "person_router"
]
