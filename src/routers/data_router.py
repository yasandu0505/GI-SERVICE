from fastapi.param_functions import Depends, Query, Path
from src.dependencies import get_config
from src.services.data_service import DataService
from src.services.opengin_service import OpenGINService
from fastapi import APIRouter

router = APIRouter(prefix="/v1/data", tags=["Data"])

def get_data_service(config: dict = Depends(get_config)):
    opengin_service = OpenGINService(config=config)
    return DataService(config, opengin_service)

@router.get('/data-catalog', summary="Get all parent/child categories and datasets.", description="Returns parent/child categories based on the given id. If id is not given it returns the top level parent categories. if id is given, it returns the categories in the below level for the given parent. if any category has a dataset, it returns the dataset as well.")
async def get_data_catalog(
    parent_id: str = Query(None, description="Parent category ID"),
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_data_catalog(parent_id)
    return service_response

@router.get('/categories/{category_id}/datasets/years', summary="Get all the dataset available years for a category.", description="Returns the list of years for available datasets")
async def get_dataset_available_years(
    category_id: str = Path(..., description="Category ID"),
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_dataset_available_years(category_id)
    return service_response