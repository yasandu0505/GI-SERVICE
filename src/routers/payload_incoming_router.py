from fastapi import APIRouter
from src.models import REQ_ONE
from src.services import IncomingService

router = APIRouter()
service = IncomingService()

# Get the relevant datasets for the ministry
@router.post("/data/{entityId}")
async def get_relevant_attributes_for_entity(REQ_ONE: REQ_ONE , entityId : str):
    extracted_data = service.incoming_payload_extractor(REQ_ONE , entityId)
    return service.expose_relevant_attributes(extracted_data)

# # Get attributes for the selected dataset
# @router.post("/data/{attributeId}")
# async def get_relevant_attributes_for_datasets(attributeId : str):
#     return