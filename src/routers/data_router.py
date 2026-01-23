

from src.models.data_requestbody import DataCatalogRequest
from fastapi.param_functions import Depends, Query, Body
from src.dependencies import get_config
from src.services.data_service import DataService
from src.services.opengin_service import OpenGINService
from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/v1/data", tags=["Data"])

def get_data_service(config: dict = Depends(get_config)):
    opengin_service = OpenGINService(config=config)
    return DataService(config, opengin_service)

@router.post('/data-catalog', summary="Get all parent/child categories and datasets.", description="Returns parent/child categories and datasets based on the given category id lists. If the list is empty, it returns the top level parent categories. If the list is not empty, it returns the categories/datasets in the next level for the given categories. The API traverses only one level.")
async def get_data_catalog(
    request: DataCatalogRequest,
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_data_catalog(request.categoryIds)
    return service_response
    