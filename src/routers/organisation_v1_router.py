from fastapi import APIRouter, Depends, Query, Body
from src.services.organisation_v1_service import OrganisationService
from src.dependencies import get_config
from src.models.organisation_v1_schemas import Date

router = APIRouter(prefix="/v1/organisation", tags=["Organisation"])

def getOrganisationService(config: dict = Depends(get_config)):
    return OrganisationService(config)

@router.post('/active-portfolio-list', summary="Get active portfolio list.", description="Returns a list of portfolios under a given president and a given date.")
async def activePortfolioList(
    presidentId: str = Query(..., description="ID of the president"),
    body: Date = Body(...),
    service: OrganisationService = Depends(getOrganisationService)
    ):
    service_response = await service.activePortfolioList(presidentId, body.date)
    return service_response