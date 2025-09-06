from fastapi import APIRouter
from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
from src.services import IncomingService

router = APIRouter()
service = IncomingService()

# Get the relevant datasets for the ministry
@router.post("/data/{entityId}")
async def get_relevant_attributes_for_entity(ENTITY_PAYLOAD: ENTITY_PAYLOAD , entityId : str):
    extracted_data = service.incoming_payload_extractor(ENTITY_PAYLOAD , entityId)
    return service.expose_relevant_attributes(extracted_data)

# Get attributes for the selected dataset
@router.post("/data/{attributeId}")
async def get_relevant_attributes_for_datasets(ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD, attributeId : str):
    return service.expose_data_for_the_attribute(ATTRIBUTE_PAYLOAD, attributeId)