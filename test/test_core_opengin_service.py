
from src.exception.exceptions import NotFoundError, BadRequestError, InternalServerError
import pytest
from unittest.mock import patch, PropertyMock, MagicMock
from aiohttp import ClientError

from services.core_opengin_service import OpenGINService
from src.utils.http_client import HTTPClient
from src.models.organisation_v1_schemas import Entity, Relation

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
async def test_get_entity_success(service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"body": [Entity(id="entity_123", name="Test Entity")]})
    
    mock_session.post.return_value = mock_response

    result = await service.get_entity(entity)

    assert result == Entity(id="entity_123", name="Test Entity")
    mock_session.post.assert_called_once()

@pytest.mark.asyncio 
async def test_get_entity_empty_entity_id(service, mock_session):
    entity = Entity(id="")
    mock_response = MockResponse({"body": []})
    
    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_none_empty_id(service, mock_session):
    entity = None
    mock_response = MockResponse({"body": []})
    
    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await service.get_entity(entity)
    
@pytest.mark.asyncio 
async def test_get_entity_by_none_response(service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"wrong_body": [Entity(id="entity_123", name="Test Entity")] })

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_empty_response(service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await service.get_entity(entity)

@pytest.mark.asyncio 
async def test_fetch_relation_success(service, mock_session):
    entity_id = "entity_123"

    mock_response = MockResponse([Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")])

    mock_session.post.return_value = mock_response

    result = await service.fetch_relation(entity_id,relation=Relation(id="relation_123",direction="OUTGOING"))

    assert result == [Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")]
    mock_session.post.assert_called_once()

@pytest.mark.asyncio 
async def test_fetch_relation_empty_entity_id(service, mock_session):
    entity_id = ""
    mock_response = MockResponse([Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")])
    
    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await service.fetch_relation(entity_id,relation=Relation(id="relation_123"))

@pytest.mark.asyncio 
async def test_fetch_relation_none_entity_id(service, mock_session):
    entity_id = None
    mock_response = MockResponse([Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")])
    
    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await service.fetch_relation(entity_id,relation=Relation(id="relation_123"))