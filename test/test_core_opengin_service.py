import pytest
from unittest.mock import patch, PropertyMock, MagicMock
from aiohttp import ClientError

from services.core_opengin_service import OpenGINService
from src.utils.http_client import HTTPClient

class MockResponse:
    def __init__(self, json_data, status=200):
        self._json_data = json_data
        self.status = status

    async def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status >= 400:
            raise ClientError("HTTP error")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

@pytest.fixture
def mock_session():
    """Fixture that provides a mocked session and patches HTTPClient"""
    session = MagicMock() 
    with patch.object(HTTPClient, 'session', new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = session
        yield session

@pytest.fixture
def service():
    """Service fixture with mocked session"""
    return OpenGINService(config={})

@pytest.mark.asyncio
async def test_get_entity_by_id_success(service, mock_session):
    entity_id = "entity_123"
    mock_response = MockResponse({"body": [{"id": entity_id, "name": "Test Entity"}]})
    
    mock_session.post.return_value = mock_response

    result = await service.get_entity_by_id(entity_id)

    assert result == {"id": entity_id, "name": "Test Entity"}
    mock_session.post.assert_called_once()