from fastapi import APIRouter
from src.models import REQ_ONE
from src.services import IncomingService

router = APIRouter()
service = IncomingService()

@router.post("/data/{ministryId}")
async def get_data(REQ_ONE: REQ_ONE , ministryId : str):
    extracted_data = service.incoming_payload_extractor(REQ_ONE , ministryId)
    return service.query_aggregator(extracted_data)

