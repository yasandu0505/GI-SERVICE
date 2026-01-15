
from src.exception.exceptions import InternalServerError
from src.models.organisation_schemas import Kind
from src.exception.exceptions import NotFoundError, BadRequestError
import pytest
from src.models.organisation_schemas import Entity, Relation
from test.conftest import MockResponse

# Test get entity
@pytest.mark.asyncio
async def test_get_entity_success(mock_service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"body": [Entity(id="entity_123", name="Test Entity")]})
    
    mock_session.post.return_value = mock_response

    result = await mock_service.get_entity(entity)

    assert result == Entity(id="entity_123", name="Test Entity")
    mock_session.post.assert_called_once()

@pytest.mark.asyncio 
async def test_get_entity_empty_entity_id(mock_service, mock_session):
    entity = Entity(id="")
    mock_response = MockResponse({"body": []})
    
    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_none_empty_id(mock_service, mock_session):
    entity = None
    mock_response = MockResponse({"body": []})
    
    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await mock_service.get_entity(entity)
    
@pytest.mark.asyncio 
async def test_get_entity_by_none_response(mock_service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"wrong_body": [Entity(id="entity_123", name="Test Entity")] })

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_by_id_empty_response(mock_service, mock_session):
    entity = Entity(id="entity_123")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_by_name_empty_response(mock_service, mock_session):
    entity = Entity(name="minister of X")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_by_kind_empty_response(mock_service, mock_session):
    kind = Kind(major="Org",minor="min")
    entity = Entity(kind=kind)
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_by_created_empty_response(mock_service, mock_session):
    entity = Entity(created="2022-12-01")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

@pytest.mark.asyncio 
async def test_get_entity_by_terminated_empty_response(mock_service, mock_session):
    entity = Entity(terminated="2022-12-01")
    mock_response = MockResponse({"body": []})

    mock_session.post.return_value = mock_response

    with pytest.raises(NotFoundError):
        await mock_service.get_entity(entity)

# Test fetch relation
@pytest.mark.asyncio 
async def test_fetch_relation_success(mock_service, mock_session):
    entity_id = "entity_123"

    mock_response = MockResponse([Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")])

    mock_session.post.return_value = mock_response

    result = await mock_service.fetch_relation(entity_id,relation=Relation(id="relation_123",direction="OUTGOING"))

    assert result == [Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")]
    mock_session.post.assert_called_once()

@pytest.mark.asyncio 
async def test_fetch_relation_empty_entity_id(mock_service, mock_session):
    entity_id = ""
    mock_response = MockResponse([Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")])
    
    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await mock_service.fetch_relation(entity_id,relation=Relation(id="relation_123"))

@pytest.mark.asyncio 
async def test_fetch_relation_none_entity_id(mock_service, mock_session):
    entity_id = None
    mock_response = MockResponse([Relation(id="relation_123",relationName="AS_MINISTER",direction="OUTGOING")])
    
    mock_session.post.return_value = mock_response

    with pytest.raises(BadRequestError):
        await mock_service.fetch_relation(entity_id,relation=Relation(id="relation_123"))





