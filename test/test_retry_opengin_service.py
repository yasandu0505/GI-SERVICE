import pytest
from src.models.organisation_schemas import Relation
from test.conftest import MockResponse
from src.models.organisation_schemas import Entity
from src.exception.exceptions import BadRequestError
from google.api_core.exceptions import InternalServerError
from unittest.mock import AsyncMock, patch
from google.api_core.exceptions import RetryError

# test retrying for get entity
@pytest.mark.asyncio
async def test_get_entity_retries_stops_on_timeout(mock_service, mock_session):
    """Test that the method retries on InternalServerError and stops after timeout"""
    entity = Entity(id="entity_123")

    mock_session.post.side_effect = InternalServerError("Connection timeout")

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        time = 0
        def fake_monotonic():
            nonlocal time
            time += 1
            return time
        with patch("time.monotonic", fake_monotonic):
            with pytest.raises(RetryError) as exc_info:
                await mock_service.get_entity(entity)

    assert exc_info.value.args[0] == "Timeout of 10.0s exceeded"

    assert mock_session.post.call_count >= 1
    assert mock_sleep.call_count >= 1

@pytest.mark.asyncio
async def test_get_entity_no_retry_on_bad_request(mock_service, mock_session):
    """Test that BadRequestError does NOT trigger retries"""
    entity = Entity(id="entity_123")
    
    mock_session.post.side_effect = BadRequestError("Bad request error")
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with patch('time.monotonic', return_value=0):
            with pytest.raises(BadRequestError):
                await mock_service.get_entity(entity)
            
            assert mock_session.post.call_count == 1
            assert mock_sleep.call_count == 0

@pytest.mark.asyncio
async def test_get_entity_succeeds_after_retries(mock_service, mock_session):
    """Test that the method eventually succeeds after retries"""
    entity = Entity(id="entity_123")
    
    success_response = MockResponse({
        "body": [{"id": "entity_123", "name": "Test Entity"}]
    })
    
    mock_session.post.return_value = success_response

    mock_session.post.side_effect = [
        InternalServerError("Network error"),
        InternalServerError("Network error"),
        success_response
    ]
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with patch('time.monotonic', return_value=0):
            result = await mock_service.get_entity(entity)
            
            assert result[0].id == "entity_123"
            assert result[0].name == "Test Entity"
            
            assert mock_session.post.call_count == 3
            assert mock_sleep.call_count == 2

# test retrying for fetch relation
@pytest.mark.asyncio
async def test_fetch_relation_retries_stops_on_timeout(mock_service, mock_session):
    """Test that the method retries on InternalServerError and stops after timeout"""
    entity_id = "entity_123"
    relation = Relation(id="relation_123",direction="OUTGOING")

    mock_session.post.side_effect = InternalServerError("Connection timeout")

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        time = 0
        def fake_monotonic():
            nonlocal time
            time += 1
            return time
        with patch("time.monotonic", fake_monotonic):
            with pytest.raises(RetryError) as exc_info:
                await mock_service.fetch_relation(entity_id,relation=relation)

    assert exc_info.value.args[0] == "Timeout of 10.0s exceeded"

    assert mock_session.post.call_count >= 1
    assert mock_sleep.call_count >= 1

@pytest.mark.asyncio
async def test_fetch_relation_no_retry_on_bad_request(mock_service, mock_session):
    """Test that BadRequestError does NOT trigger retries"""
    entity_id = "entity_123"
    relation = Relation(id="relation_123",direction="OUTGOING")
    
    mock_session.post.side_effect = BadRequestError("Bad request error")
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with patch('time.monotonic', return_value=0):
            with pytest.raises(BadRequestError):
                await mock_service.fetch_relation(entity_id, relation=relation)
            
            assert mock_session.post.call_count == 1
            assert mock_sleep.call_count == 0

@pytest.mark.asyncio
async def test_fetch_relation_succeeds_after_retries(mock_service, mock_session):
    """Test that the method eventually succeeds after retries"""
    entity_id = "entity_123"
    relation = Relation(id="relation_123",direction="OUTGOING")
    
    success_response = MockResponse([Relation(id="relation_123",name="AS_MINISTER",direction="OUTGOING")])
    
    mock_session.post.return_value = success_response

    mock_session.post.side_effect = [
        InternalServerError("Network error"),
        InternalServerError("Network error"),
        success_response
    ]
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with patch('time.monotonic', return_value=0):
            result = await mock_service.fetch_relation(entity_id,relation=relation)
            
            result_first_datum = result[0]

            assert result_first_datum.id == "relation_123"
            assert result_first_datum.name == "AS_MINISTER"
            assert result_first_datum.direction == "OUTGOING"
            
            assert mock_session.post.call_count == 3
            assert mock_sleep.call_count == 2

# test retrying for get metadata
@pytest.mark.asyncio
async def test_get_metadata_retries_stops_on_timeout(mock_service, mock_session):
    """Test that the method retries on InternalServerError and stops after timeout"""
    category_id = "category_123"

    mock_session.get.side_effect = InternalServerError("Connection timeout")

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        time = 0
        def fake_monotonic():
            nonlocal time
            time += 1
            return time
        with patch("time.monotonic", fake_monotonic):
            with pytest.raises(RetryError) as exc_info:
                await mock_service.get_metadata(category_id)

    assert exc_info.value.args[0] == "Timeout of 10.0s exceeded"

    assert mock_session.get.call_count >= 1
    assert mock_sleep.call_count >= 1

@pytest.mark.asyncio
async def test_get_metadata_no_retry_on_bad_request(mock_service, mock_session):
    """Test that BadRequestError does NOT trigger retries"""
    category_id = "category_123"
    
    mock_session.get.side_effect = BadRequestError("Bad request error")
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with patch('time.monotonic', return_value=0):
            with pytest.raises(BadRequestError):
                await mock_service.get_metadata(category_id)
            
            assert mock_session.get.call_count == 1
            assert mock_sleep.call_count == 0

@pytest.mark.asyncio
async def test_get_metadata_succeeds_after_retries(mock_service, mock_session):
    """Test that the method eventually succeeds after retries"""
    category_id = "category_123"
    
    success_response = MockResponse({"key1": "value1", "key2": "value2"})
    
    mock_session.get.return_value = success_response

    mock_session.get.side_effect = [
        InternalServerError("Network error"),
        InternalServerError("Network error"),
        success_response
    ]
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with patch('time.monotonic', return_value=0):
            result = await mock_service.get_metadata(category_id)
            
            assert result["key1"] == "value1"
            assert result["key2"] == "value2"   
            
            assert mock_session.get.call_count == 3
            assert mock_sleep.call_count == 2
            

