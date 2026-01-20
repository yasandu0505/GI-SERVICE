
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

    assert result == [Entity(id="entity_123", name="Test Entity")]
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

# Tests for get_metadata
@pytest.mark.asyncio
async def test_get_metadata_success(mock_service, mock_session):
    """Test get_metadata with successful response"""
    category_id = "category_123"
    metadata_response = {
        "attr1": "value1",
        "attr2": "value2",
        "description": "Test metadata"
    }
    
    mock_response = MockResponse(metadata_response)
    mock_session.get.return_value = mock_response
    
    result = await mock_service.get_metadata(category_id)
    
    assert result == metadata_response
    assert result["attr1"] == "value1"
    assert result["attr2"] == "value2"
    mock_session.get.assert_called_once()

@pytest.mark.asyncio
async def test_get_metadata_empty_response(mock_service, mock_session):
    """Test get_metadata with empty metadata response"""
    category_id = "category_456"
    empty_metadata = {}
    
    mock_response = MockResponse(empty_metadata)
    mock_session.get.return_value = mock_response
    
    result = await mock_service.get_metadata(category_id)
    
    assert result == {}
    mock_session.get.assert_called_once()

@pytest.mark.asyncio
async def test_get_metadata_http_error(mock_service, mock_session):
    """Test get_metadata handles HTTP errors"""
    from src.exception.exceptions import InternalServerError
    category_id = "category_error"
    
    mock_response = MockResponse({}, status=500)
    mock_session.get.return_value = mock_response
    
    with pytest.raises(InternalServerError) as exc_info:
        await mock_service.get_metadata(category_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_get_metadata_not_found(mock_service, mock_session):
    """Test get_metadata handles 404 errors"""
    from src.exception.exceptions import InternalServerError
    category_id = "nonexistent_category"
    
    mock_response = MockResponse({}, status=404)
    mock_session.get.return_value = mock_response
    
    with pytest.raises(InternalServerError) as exc_info:
        await mock_service.get_metadata(category_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_get_metadata_with_exception(mock_service, mock_session):
    """Test get_metadata handles general exceptions"""
    from src.exception.exceptions import InternalServerError
    category_id = "category_exception"
    
    # Simulate an exception during the request
    mock_session.get.side_effect = Exception("Network timeout")
    
    with pytest.raises(InternalServerError) as exc_info:
        await mock_service.get_metadata(category_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)
    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == "Network timeout"
