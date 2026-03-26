from fastapi import APIRouter, Depends, Path
from src.services import OpenGINService, PersonService

router = APIRouter(prefix="/v1/person", tags=["Person"])

<<<<<<< HEAD
def get_person_service():
    opengin_service = OpenGINService()
    return PersonService(opengin_service)
=======

def get_person_service(config: dict = Depends(get_config)):
    opengin_service = OpenGINService(config=config)
    return PersonService(config, opengin_service)
>>>>>>> c92f16b (Refactor enums)


@router.get(
    "/person-history/{person_id}",
    summary="Get person history.",
    description="Returns a ministry history and president history for a given person.",
)
async def person_history(
    person_id: str = Path(..., description="ID of the person"),
    service: PersonService = Depends(get_person_service),
):
    service_response = await service.fetch_person_history(person_id)
    return service_response


@router.get(
    "/person-profile/{person_id}",
    summary="Get person profile.",
    description="Returns a person profile for a given person.",
)
async def person_profile(
    person_id: str = Path(..., description="ID of the person"),
    service: PersonService = Depends(get_person_service),
):
    service_response = await service.fetch_person_profile(person_id)
    return service_response


@router.get(
    "/all-presidents",
    summary="Get all presidents.",
    description="Returns a list of all presidents with their tenures and gazettes published under each.",
)
async def all_presidents(service: PersonService = Depends(get_person_service)):
    service_response = await service.fetch_all_presidents()
    return service_response
