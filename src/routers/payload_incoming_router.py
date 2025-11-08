from fastapi import APIRouter, Depends
from src.models import ENTITY_PAYLOAD, ATTRIBUTE_PAYLOAD, WRITE_PAYLOAD
from src.services import IncomingServiceAttributes, WriteAttributes
from src.dependencies import get_config
from chartFactory.utils import transform_data_for_chart
from aiohttp import ClientSession, ClientTimeout
from typing import AsyncGenerator

router = APIRouter()
# writer = WriteAttributes() 

def get_stat_service(config: dict = Depends(get_config)):
    return IncomingServiceAttributes(config)

def get_writer_service(config: dict = Depends(get_config)):
    return WriteAttributes(config)

async def get_http_session() -> AsyncGenerator[ClientSession, None]:
    timeout = ClientTimeout(total=90, connect=30, sock_connect=30, sock_read=90)
    async with ClientSession(timeout=timeout) as session:
        yield session

@router.get("/allAttributes")
async def get_all_attributes(statService: IncomingServiceAttributes = Depends(get_stat_service)):
    return statService.expose_all_attributes()
@router.get("/categories")
async def get_all_categories(
    id: str | None = None,
    statService: IncomingServiceAttributes = Depends(get_stat_service),
    session: ClientSession = Depends(get_http_session),
):
    return await statService.expose_category_by_id(id, session)

# Get the relevant attributes for the entity
@router.post("/data/entity/{entityId}")
async def get_relevant_attributes_for_entity(
    ENTITY_PAYLOAD: ENTITY_PAYLOAD,
    entityId : str,
    statService: IncomingServiceAttributes = Depends(get_stat_service),
    session: ClientSession = Depends(get_http_session),
):
    attributes_of_the_entity = await statService.expose_relevant_attributes(ENTITY_PAYLOAD , entityId, session)
    return attributes_of_the_entity

# Get attributes for the selected attribute
@router.post("/data/attribute/{entityId}")
async def get_relevant_attributes_for_datasets(
    ATTRIBUTE_PAYLOAD: ATTRIBUTE_PAYLOAD,
    entityId : str,
    statService: IncomingServiceAttributes = Depends(get_stat_service),
    session: ClientSession = Depends(get_http_session),
):
    attribute_data_out = await statService.expose_data_for_the_attribute(ATTRIBUTE_PAYLOAD, entityId, session)
    return transform_data_for_chart(attribute_data_out)

# Write attributes to the entities
@router.post("/data/writeAttributes")
async def write_attributes(WRITE_PAYLOAD: WRITE_PAYLOAD, writer: WriteAttributes = Depends(get_writer_service)):
    # Example : base_url = /Users/yasandu/Desktop/datasets/data/2022
    base_url = WRITE_PAYLOAD.base_url
    result = writer.traverse_folder(base_url)
    result = writer.pre_process_traverse_result(result)
    result = writer.entity_validator(result)
    # return result
    return writer.create_categories_and_insert_datasets(result)

@router.get("/data/writeMetadata")
async def write_metadata(writer: WriteAttributes = Depends(get_writer_service)):
    success, collection_names , db = writer.connect_to_mongodb()
    if success:
        documents = writer.get_all_documents_from_nexoan()
        categorised_documents = writer.categorise_documents_by_year(documents)
        return writer.add_metadata_to_the_document(categorised_documents, db)
    else:
        return "❌ Could not connect to MongoDB"
    
    return writer.create_parent_categories_and_children_categories_v2(result)

@router.get("/data/yearswithdata")
async def yearswithdata(
    name: str,
    parentId: str,
    statService: IncomingServiceAttributes = Depends(get_stat_service),
    session: ClientSession = Depends(get_http_session),
):
    years = await statService.datacategoriesbyyear(name, parentId, session)
    return years

@router.get("/data/orgchart/{entityId}/ministers-departments")
async def get_ministers_and_departments(
    entityId: str,
    activeDate: str,
    statService: IncomingServiceAttributes = Depends(get_stat_service),
    session: ClientSession = Depends(get_http_session),
):
    """
    Test endpoint for get_ministers_and_departments function.
    
    Parameters:
    - entityId: The entity ID to get ministers for (e.g., president ID)
    - activeDate: Date active (e.g., "2023-05-15")
    """
    result = await statService.get_ministers_and_departments(entityId, activeDate, session)
    return result

# Get the timeline for the orgchart
# @router.get("/data/orgchart/timeline")
# async def get_timeline_for_orgchart(orgchartService: IncomingServiceOrgchart = Depends(get_orgchart_service)):
#     documentData = await orgchartService.get_documents()
#     presidentData = await orgchartService.get_presidents()
#     timeLine = orgchartService.get_timeline(documentData, presidentData)
#     return timeLine

# # Get ministries for the selected date
# @router.post("/data/orgchart/ministries")
# async def get__for_orgchart(orgchartService: IncomingServiceOrgchart = Depends(get_orgchart_service)):
#     return

# # Get departments for the selected ministry at the selected date
# @router.post("/data/orgchart/departments")
# async def get__for_orgchart(orgchartService: IncomingServiceOrgchart = Depends(get_orgchart_service)):
#     return
