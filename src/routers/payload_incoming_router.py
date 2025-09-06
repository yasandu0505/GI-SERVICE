from fastapi import APIRouter
from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
from src.services import IncomingService

router = APIRouter()
service = IncomingService()

# Get the relevant attributes for the entity
@router.post("/data/entity/{entityId}")
async def get_relevant_attributes_for_entity(ENTITY_PAYLOAD: ENTITY_PAYLOAD , entityId : str):
    extracted_data = service.incoming_payload_extractor(ENTITY_PAYLOAD , entityId)
    return service.expose_relevant_attributes(extracted_data)

# Get attributes for the selected attribute
@router.post("/data/attribute/{attributeId}")
async def get_relevant_attributes_for_datasets(ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD, attributeId : str):
    # datasetOUT= service.expose_data_for_the_attribute(ATTRIBUTE_PAYLOAD, attributeId)
    mock_api_response =  {
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "",
                        "value": {
                            "employee_table": {
                                "columns": ["id", "name", "age", "department", "salary"],
                                "rows": [
                                    [1, "John Doe", 30, "Engineering", 75000.50],
                                    [2, "Jane Smith", 25, "Marketing", 65000],
                                    [3, "Bob Wilson", 35, "Sales", 85000.75],
                                    [4, "Alice Brown", 28, "Engineering", 70000.25],
                                    [5, "Charlie Davis", 32, "Finance", 80000]
                                ]
                            }
                        }
                    }    
    return service.data_transforming(mock_api_response)

