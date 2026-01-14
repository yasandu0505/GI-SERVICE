

from fastapi.param_functions import Depends, Query
from src.dependencies import get_config
from src.services.data_service import DataService
from src.services.opengin_service import OpenGINService
from fastapi import APIRouter

router = APIRouter(prefix="/v1/data", tags=["Data"])

def get_data_service(config: dict = Depends(get_config)):
    opengin_service = OpenGINService(config=config)
    return DataService(config, opengin_service)

@router.get('/data-catalog', summary="Get all parent/child categories and datasets.", description="Returns parent/child categories and datasets based on the given entity id. If entity id is not given it returns the top level parent categories. if entity id is given, it returns the categories and the datasets in the below level for the given parent.")
async def get_data_catalog(
    entity_id: str = Query(None, description="Entity ID"),
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_data_catalog(entity_id)
    return service_response
    