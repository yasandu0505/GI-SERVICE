from fastapi import APIRouter, Depends
from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD
from src.services import IncomingService
from src.services import IncomingServiceOrgchart
from src.utils import CacheService
from src.dependencies import get_cache
from chartFactory.utils import transform_data_for_chart

router = APIRouter()
statService  = IncomingService()

def get_service(cache: CacheService = Depends(get_cache)):
    return IncomingServiceOrgchart(cache)

# Get the relevant attributes for the entity
@router.post("/data/entity/{entityId}")
async def get_relevant_attributes_for_entity(ENTITY_PAYLOAD: ENTITY_PAYLOAD , entityId : str):
    extracted_data = statService.incoming_payload_extractor(ENTITY_PAYLOAD , entityId)
    attributes_of_the_entity = await statService.expose_relevant_attributes(extracted_data)
    return attributes_of_the_entity

# Get attributes for the selected attribute
@router.post("/data/attribute/{entityId}")
async def get_relevant_attributes_for_datasets(ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD, entityId : str):
    chart_type = ATTRIBUTE_PAYLOAD.chart_type
    x_axis = ATTRIBUTE_PAYLOAD.x_axis or None
    y_axis = ATTRIBUTE_PAYLOAD.y_axis or None
    label = ATTRIBUTE_PAYLOAD.label or None
    value = ATTRIBUTE_PAYLOAD.value or None
    # datasetOUT= statService.expose_data_for_the_attribute(ATTRIBUTE_PAYLOAD, entityId)
    mock_api_response =  {
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "",
                        "value": {
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
    try:
        data = transform_data_for_chart(mock_api_response, chart_type, x_axis, y_axis, label, value)
    except ValueError as e:
        data = {"error": f"{str(e)}"}
     
    return data

@router.get("/data/orgchart/timeline")
async def get_timeline_for_orgchart(orgchartService: IncomingServiceOrgchart = Depends(get_service)):
    documentData = await orgchartService.get_documents()
    presidentData = await orgchartService.get_presidents()
    timeLine = orgchartService.get_timeline(documentData, presidentData)
    return timeLine


