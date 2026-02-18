import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from src.models.organisation_schemas import Entity, Relation
from src.exception.exceptions import BadRequestError, InternalServerError

# --- Tests for is_president_during ---

@pytest.mark.asyncio
async def test_is_president_during_true(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2021-01-01T00:00:00Z"
    president_relations = [Relation(startTime="2019-01-01T00:00:00Z", endTime="2020-06-01T00:00:00Z")]
    result = person_service.is_president_during(president_relations, ministry_start, ministry_end)
    assert result is True

@pytest.mark.asyncio
async def test_is_president_during_false(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2021-01-01T00:00:00Z"
    president_relations = [Relation(startTime="2022-01-01T00:00:00Z", endTime="2023-01-01T00:00:00Z")]
    result = person_service.is_president_during(president_relations, ministry_start, ministry_end)
    assert result is False

@pytest.mark.asyncio
async def test_is_president_during_ongoing_presidency(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2021-01-01T00:00:00Z"
    president_relations = [Relation(startTime="2020-06-01T00:00:00Z", endTime="")] # Ongoing is empty string
    result = person_service.is_president_during(president_relations, ministry_start, ministry_end)
    assert result is True

@pytest.mark.asyncio
async def test_is_president_during_ongoing_ministry(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "" # Ongoing ministry
    president_relations = [Relation(startTime="2020-06-01T00:00:00Z", endTime="2020-12-01T00:00:00Z")]
    result = person_service.is_president_during(president_relations, ministry_start, ministry_end)
    assert result is True

@pytest.mark.asyncio
async def test_is_president_during_exact_touch(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2020-06-01T00:00:00Z"
    president_relations = [Relation(startTime="2020-06-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")]
    # ministry ends same day presidency starts
    result = person_service.is_president_during(president_relations, ministry_start, ministry_end)
    assert result is False

# --- Tests for fetch_person_history ---

@pytest.mark.asyncio
async def test_fetch_person_history_success(person_service, mock_opengin_service):
    person_id = "person_123"
    mock_opengin_service.fetch_relation.side_effect = [
        [Relation(relatedEntityId="min_1", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")], 
        [] # No president relations
    ]
    with patch.object(person_service, 'enrich_history_item', new_callable=AsyncMock) as mock_enrich:
        mock_enrich.return_value = {"id": "min_1", "name": "Min", "term": "T", "is_president": False, "start_time": "2020", "end_time": "2021"}
        result = await person_service.fetch_person_history(person_id)
        assert result["ministries_worked_at"] == 1
        assert len(result["ministry_history"]) == 1

@pytest.mark.asyncio
async def test_fetch_person_history_sorting(person_service, mock_opengin_service):
    person_id = "person_123"
    mock_opengin_service.fetch_relation.side_effect = [
        [
            Relation(relatedEntityId="old", startTime="2010-01-01T00:00:00Z", endTime="2012-01-01T00:00:00Z"),
            Relation(relatedEntityId="ongoing", startTime="2022-01-01T00:00:00Z", endTime=""),
            Relation(relatedEntityId="recent", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")
        ],
        []
    ]
    
    # We need to mock enrich_history_item to return items with different end times
    async def side_effect(rel, pres_rels):
        return {
            "id": rel.relatedEntityId,
            "start_time": rel.startTime,
            "end_time": rel.endTime
        }
    
    with patch.object(person_service, 'enrich_history_item', side_effect=side_effect):
        result = await person_service.fetch_person_history(person_id)
        history = result["ministry_history"]
        
        # Expected order: ongoing (""), then 2021, then 2012
        assert history[0]["id"] == "ongoing"
        assert history[1]["id"] == "recent"
        assert history[2]["id"] == "old"

@pytest.mark.asyncio
async def test_fetch_person_history_no_ministries(person_service, mock_opengin_service):
    person_id = "person_123"
    mock_opengin_service.fetch_relation.return_value = []
    result = await person_service.fetch_person_history(person_id)
    assert result["ministries_worked_at"] == 0
    assert result["ministry_history"] == []

@pytest.mark.asyncio
async def test_fetch_person_history_internal_error(person_service, mock_opengin_service):
    person_id = "person_123"
    with patch("src.services.person_service.asyncio.gather", side_effect=Exception("Gather fail")):
        with pytest.raises(InternalServerError):
            await person_service.fetch_person_history(person_id)

@pytest.mark.asyncio
async def test_fetch_person_history_bad_request(person_service):
    with pytest.raises(BadRequestError):
        await person_service.fetch_person_history("")

# --- Tests for enrich_history_item ---

@pytest.mark.asyncio
async def test_enrich_history_item_success(person_service, mock_opengin_service):
    person_id = "person_123"
    relation = Relation(relatedEntityId="min_1", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")
    
    mock_opengin_service.get_entities.return_value = [Entity(id="min_1", name="raw_name")]
    
    with patch("src.services.person_service.Util.decode_protobuf_attribute_name", return_value="Ministry of Magic"), \
         patch("src.services.person_service.Util.term", return_value="2020-01-01 - 2021-01-01"):
        
        result = await person_service.enrich_history_item(relation, [])
        assert result["id"] == "min_1"
        assert result["name"] == "Ministry of Magic"
        assert result["is_president"] is False

@pytest.mark.asyncio
async def test_enrich_history_item_not_found(person_service, mock_opengin_service):
    person_id = "person_123"
    relation = Relation(relatedEntityId="ghost", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")
    
    mock_opengin_service.get_entities.return_value = [] # Not found
    
    result = await person_service.enrich_history_item(relation, [])
    assert result is None

@pytest.mark.asyncio
async def test_enrich_history_item_error(person_service, mock_opengin_service):
    person_id = "person_123"
    relation = Relation(relatedEntityId="min_1", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")
    
    mock_opengin_service.get_entities.side_effect = Exception("Timeout")
    
    result = await person_service.enrich_history_item(relation, [])
    assert result is None # Service returns None on error within enrichment
