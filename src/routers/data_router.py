from src.models.data_requestbody import DatasetYearsRequest
from src.models.data_requestbody import DataCatalogRequest
from fastapi.param_functions import Depends, Query, Body, Path
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

@router.post('/datasets/years', summary="Get all the dataset available years for the given datasets.", description="Returns the list of years and datasetIds for available datasets with dataset name.")
async def get_dataset_available_years(
    request: DatasetYearsRequest,
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_dataset_available_years(request.datasetIds)
    return service_response

@router.get('/datasets/{datasetId}/data', summary="Get the data attributes for the given dataset.", description="Returns the data attributes for the given dataset")
async def get_data_attributes(
    datasetId: str = Path(..., description="The ID of the dataset"),
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_data_attributes(datasetId)
    return service_response

@router.get('/datasets/{datasetId}/root', summary="Get the root of the given dataset.", description="Returns the root of the given dataset.")
async def get_dataset_root(
    datasetId: str = Path(..., description="The ID of the dataset"),
    service: DataService = Depends(get_data_service)
):
    service_response = await service.fetch_dataset_root(datasetId)
    return service_response    
