from fastapi import APIRouter, Depends, Query, Body
from src.dependencies import get_config
from src.models.organisation_v1_schemas import Date
from src.services import OpenGINService, OrganisationService

router = APIRouter(prefix="/v1/organisation", tags=["Organisation"])

def get_organisation_service(config: dict = Depends(get_config)):
    opengin_service = OpenGINService(config=config)
    return OrganisationService(config, opengin_service)

@router.post('/active-portfolio-list', summary="Get active portfolio list.", description="Returns a list of portfolios under a given president and a given date.")
async def active_portfolio_list(
    presidentId: str = Query(..., description="ID of the president"),
    body: Date = Body(...),
    service: OrganisationService = Depends(get_organisation_service)
    ):
    service_response = await service.active_portfolio_list(presidentId, body.date)
    return service_response