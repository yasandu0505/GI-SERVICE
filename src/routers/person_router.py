from fastapi import APIRouter, Depends, Query, Body, Path
from src.dependencies import get_config
from src.services import OpenGINService, PersonService

router = APIRouter(prefix="/v1/person", tags=["Person"])

def get_person_service(config: dict = Depends(get_config)):
    opengin_service = OpenGINService(config=config)
    return PersonService(config, opengin_service)

@router.get('/person-history/{person_id}', summary="Get person history.", description="Returns a ministry history and president history for a given person.")
async def person_history(
    person_id: str = Path(..., description="ID of the person"),
    service: PersonService = Depends(get_person_service)
):
    service_response = await service.fetch_person_history(person_id)
    return service_response