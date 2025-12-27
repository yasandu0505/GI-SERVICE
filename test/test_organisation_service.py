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
            is_president=True
        )

    assert result == {
        "id": president_id,
        "name": "John Doe",
        "isNew": False,
        "isPresident": True
    }

    mock_opengin_service.get_entity_by_id.assert_called_once_with(entityId=president_id)