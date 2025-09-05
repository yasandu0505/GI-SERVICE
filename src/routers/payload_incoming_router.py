from fastapi import APIRouter
from src.models import REQ_ONE
from src.services import IncomingService

router = APIRouter()
service = IncomingService()

@router.post("/data/{ministryId}")
async def get_data(REQ_ONE: REQ_ONE , ministryId : str):
    return service.incoming_payload_extractor(REQ_ONE , ministryId)
