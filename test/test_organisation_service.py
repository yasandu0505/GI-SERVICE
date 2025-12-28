from src.exception.exceptions import BadRequestError
import pytest
from unittest.mock import AsyncMock, patch
from services.organisation_v1_service import OrganisationService

@pytest.fixture
def mock_opengin_service():
    return AsyncMock()

@pytest.fixture
def organisation_service(mock_opengin_service):
    config = {}
    return OrganisationService(config, mock_opengin_service)

@pytest.mark.asyncio
async def test_enrich_person_data_as_president(organisation_service, mock_opengin_service):
    selected_date = "2023-10-27"
    president_id = "pres_123"
    is_president = True

    mock_opengin_service.get_entity_by_id.return_value = {
        "id": president_id,
        "name": "mocked_protobuf_name"
    }

    with patch(
        "services.organisation_v1_service.decode_protobuf_attribute_name",
        return_value="John Doe"
    ):
        result = await organisation_service.enrich_person_data(
            selected_date=selected_date,
            president_id=president_id,
            is_president=is_president
        )

    assert result == {
        "id": president_id,
        "name": "John Doe",
        "isNew": False,
        "isPresident": True
    }

    mock_opengin_service.get_entity_by_id.assert_called_once_with(entity={"id": president_id})

@pytest.mark.asyncio 
async def test_enrich_person_data_as_not_president(organisation_service, mock_opengin_service):
    selected_date = "2023-10-27"
    president_id = "pres_123"
    person_relation = {"relatedEntityId": "person_123","startTime": "2023-10-27T00:00:00Z","endTime": "2024-10-27T00:00:00Z"}

    mock_opengin_service.get_entity_by_id.return_value = {
        "id": "person_123",
        "name": "mocked_protobuf_name"
    }

    with patch(
        "services.organisation_v1_service.decode_protobuf_attribute_name",
        return_value="John Doe"
    ):
        result = await organisation_service.enrich_person_data(
            selected_date=selected_date,
            president_id=president_id,
            person_relation=person_relation
        )

    assert result == {
        "id": "person_123",
        "name": "John Doe",
        "isNew": True,
        "isPresident": False
    }

    mock_opengin_service.get_entity_by_id.assert_called_once_with(entity={"id": person_relation['relatedEntityId']})

@pytest.mark.asyncio 
async def test_enrich_department_item(organisation_service, mock_opengin_service):
    department_relation = {"relatedEntityId": "department_123","startTime": "2023-10-27T00:00:00Z","endTime": "2024-10-27T00:00:00Z"}
    selected_date = "2023-10-27"

    mock_opengin_service.get_entity_by_id.return_value = {
        "id":"department_123",
        "kind": {"major":"org","minor":"dep"},
        "name": "Department_of_security",
        "created": "2023-10-27T00:00:00Z",
        "terminated": ""
    }

    mock_opengin_service.fetch_relation.return_value = [
            {
                "id": "",
                "relatedEntityId": "department_123",
                "name": "AS_CATEGORY",
                "startTime": "2020-08-09T00:00:00Z",
                "endTime": "2022-03-08T00:00:00Z",
                "direction": "OUTGOING"
            }
        ]
    
    with patch(
        "services.organisation_v1_service.decode_protobuf_attribute_name",
        return_value="Department_of_security"
    ):
        result = await organisation_service.enrich_department_item(
            selected_date=selected_date,
            department_relation=department_relation
        )

    assert result == {
        "id": "department_123",
        "name": "Department_of_security",
        "isNew": True,
        "hasData": True
    }

    mock_opengin_service.get_entity_by_id.assert_called_once_with(entity={"id": department_relation['relatedEntityId']})

@pytest.mark.asyncio 
async def test_enrich_department_item_with_no_data(organisation_service, mock_opengin_service):
    department_relation = {"relatedEntityId": "department_123","startTime": "2023-10-27T00:00:00Z","endTime": "2024-10-27T00:00:00Z"}
    selected_date = "2023-10-27"

    mock_opengin_service.get_entity_by_id.return_value = {
        "id":"department_123",
        "kind": {"major":"org","minor":"dep"},
        "name": "Department_of_security",
        "created": "2023-10-27T00:00:00Z",
        "terminated": ""
    }

    mock_opengin_service.fetch_relation.return_value = []
    
    with patch(
        "services.organisation_v1_service.decode_protobuf_attribute_name",
        return_value="Department_of_security"
    ):
        result = await organisation_service.enrich_department_item(
            selected_date=selected_date,
            department_relation=department_relation
        )

    assert result == {
        "id": "department_123",
        "name": "Department_of_security",
        "isNew": True,
        "hasData": False
    }

    mock_opengin_service.get_entity_by_id.assert_called_once_with(entity={"id": department_relation['relatedEntityId']})

@pytest.mark.asyncio 
async def test_enrich_department_item_not_new(organisation_service, mock_opengin_service):
    department_relation = {"relatedEntityId": "department_123","startTime": "2023-10-27T00:00:00Z","endTime": "2024-10-27T00:00:00Z"}
    selected_date = "2024-10-27"

    mock_opengin_service.get_entity_by_id.return_value = {
        "id":"department_123",
        "kind": {"major":"org","minor":"dep"},
        "name": "Department_of_security",
        "created": "2023-10-27T00:00:00Z",
        "terminated": ""
    }

    mock_opengin_service.fetch_relation.return_value = []
    
    with patch(
        "services.organisation_v1_service.decode_protobuf_attribute_name",
        return_value="Department_of_security"
    ):
        result = await organisation_service.enrich_department_item(
            selected_date=selected_date,
            department_relation=department_relation
        )

    assert result == {
        "id": "department_123",
        "name": "Department_of_security",
        "isNew": False,
        "hasData": False
    }

    mock_opengin_service.get_entity_by_id.assert_called_once_with(entity={"id": department_relation['relatedEntityId']})
        
@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_success(organisation_service, mock_opengin_service):
    portfolio_id = "portfolio_123"
    selected_date = "2021-10-27"

    mock_opengin_service.fetch_relation.return_value = [
            {
                "id": "",
                "relatedEntityId": "department_123",
                "name": "AS_DEPARTMENT",
                "startTime": "2020-08-09T00:00:00Z",
                "endTime": "2022-03-08T00:00:00Z",
                "direction": "OUTGOING"
            }
        ]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "services.organisation_v1_service.OrganisationService.enrich_department_item",
        new_callable=AsyncMock
    ) as mock_enrich_department:
        mock_enrich_department.return_value = {
            "id": "department_123",
            "name": "Department_of_security",
            "isNew": False,
            "hasData": False
        }

        result = await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

    assert result == {
        "totalDepartments": 1,
        "newDepartments": 0,
        "departmentList": [
            {
                "id": "department_123",
                "name": "Department_of_security",
                "isNew": False,
                "hasData": False
            }
        ]
    }

    # Check fetch_relation was called correctly
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=portfolio_id,
        relationName="AS_DEPARTMENT",
        activeAt=f"{selected_date}T00:00:00Z"
    )

    # Ensure enrich_department_item was called once with the correct args
    mock_enrich_department.assert_called_once_with(
        department_relation=mock_opengin_service.fetch_relation.return_value[0],
        selected_date=selected_date
    )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_empty_portfolio_id(organisation_service, mock_opengin_service):
    portfolio_id = ""
    selected_date = "2021-10-27"

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_none_portfolio_id(organisation_service, mock_opengin_service):
    portfolio_id = None
    selected_date = "2021-10-27"

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_empty_selected_date(organisation_service, mock_opengin_service):
    portfolio_id = "portfolio_123"
    selected_date = ""

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_none_selected_date(organisation_service, mock_opengin_service):
    portfolio_id = "portfolio_123"
    selected_date = None

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

    