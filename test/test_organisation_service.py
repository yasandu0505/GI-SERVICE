import pytest
from src.exception.exceptions import InternalServerError
from src.utils.util_functions import Util
from src.exception.exceptions import NotFoundError
from src.exception.exceptions import BadRequestError
from unittest.mock import AsyncMock, patch
from src.models.organisation_schemas import Entity, Relation

@pytest.mark.asyncio
async def test_enrich_person_data_as_president(organisation_service, mock_opengin_service):
    selected_date = "2023-10-27"
    president_id = "pres_123"
    is_president = True

    mock_opengin_service.get_entities.return_value = [Entity(id=president_id,name="mocked_protobuf_name")]

    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
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

    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=president_id))

@pytest.mark.asyncio 
async def test_enrich_person_data_as_not_president(organisation_service, mock_opengin_service):
    selected_date = "2023-10-27"
    president_id = "pres_123"
    person_relation = Relation(relatedEntityId="person_123",startTime="2023-10-27T00:00:00Z",endTime="2024-10-27T00:00:00Z")

    mock_opengin_service.get_entities.return_value = [Entity(id="person_123",name="mocked_protobuf_name")]

    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
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

    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=person_relation.relatedEntityId))

@pytest.mark.asyncio 
async def test_enrich_department_item(organisation_service, mock_opengin_service):
    department_relation = Relation(relatedEntityId="department_123", startTime="2023-10-27T00:00:00Z", endTime="2024-10-27T00:00:00Z")
    selected_date = "2023-10-27"

    mock_opengin_service.get_entities.return_value = [Entity(id="department_123",name="mocked_protobuf_name")]

    mock_opengin_service.fetch_relation.return_value = [ Relation(id="", relatedEntityId="department_123", name= "AS_CATEGORY", startTime="2020-08-09T00:00:00Z", endTime="2022-03-08T00:00:00Z", direction="OUTGOING")]
    
    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
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

    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=department_relation.relatedEntityId))

@pytest.mark.asyncio 
async def test_enrich_department_item_with_no_data(organisation_service, mock_opengin_service):
    department_relation = Relation(relatedEntityId="department_123", startTime="2023-10-27T00:00:00Z", endTime="2024-10-27T00:00:00Z")
    selected_date = "2023-10-27"

    mock_opengin_service.get_entities.return_value =  [Entity(id="department_123",name="mocked_protobuf_name")]

    mock_opengin_service.fetch_relation.return_value = []
    
    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
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

    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=department_relation.relatedEntityId))

@pytest.mark.asyncio 
async def test_enrich_department_item_not_new(organisation_service, mock_opengin_service):
    department_relation = Relation(relatedEntityId="department_123", startTime="2023-10-27T00:00:00Z", endTime="2024-10-27T00:00:00Z")
    selected_date = "2024-10-27"

    mock_opengin_service.get_entities.return_value = [Entity(id="department_123",name="mocked_protobuf_name")]

    mock_opengin_service.fetch_relation.return_value = []
    
    with patch(
        "services.organisation_service.Util.decode_protobuf_attribute_name",
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

    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=department_relation.relatedEntityId))
        
@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_success(organisation_service, mock_opengin_service):
    portfolio_id = "portfolio_123"
    selected_date = "2021-10-27"

    mock_opengin_service.fetch_relation.return_value = [ Relation(id="", relatedEntityId="portfolio_123", name= "AS_DEPARTMENT", startTime="2020-08-09T00:00:00Z", endTime="2022-03-08T00:00:00Z", direction="OUTGOING")]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "services.organisation_service.OrganisationService.enrich_department_item",
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
        relation=Relation(name= "AS_DEPARTMENT", activeAt=f'{selected_date}T00:00:00Z', direction="OUTGOING")
    )

    # Ensure enrich_department_item was called once with the correct args
    mock_enrich_department.assert_called_once_with(
        department_relation=mock_opengin_service.fetch_relation.return_value[0],
        selected_date=selected_date
    )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_empty_portfolio_id(organisation_service):
    portfolio_id = ""
    selected_date = "2021-10-27"

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_none_portfolio_id(organisation_service):
    portfolio_id = None
    selected_date = "2021-10-27"

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_empty_selected_date(organisation_service):
    portfolio_id = "portfolio_123"
    selected_date = ""

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_departments_by_portfolio_id_none_selected_date(organisation_service):
    portfolio_id = "portfolio_123"
    selected_date = None

    with pytest.raises(BadRequestError):
        await organisation_service.departments_by_portfolio(
            portfolio_id=portfolio_id,
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_prime_minister_success(organisation_service, mock_opengin_service):
    selected_date = "2021-10-27"

    mock_response = Relation(name='AS_PRIME_MINISTER', activeAt='', relatedEntityId='cit_3', startTime='2022-07-26T00:00:00Z', endTime='2024-09-23T00:00:00Z', id='person_123', direction='OUTGOING')
    mock_opengin_service.fetch_relation.return_value = [mock_response]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {
            "id": "person_123",
            "name": "Person X",
            "isNew": False,
            "isPresident": False
        }

        result = await organisation_service.fetch_prime_minister(selected_date=selected_date)

    assert result == {
            "body": {
                "id": "person_123",
                "name": "Person X",
                "isNew": False,
                "term": "2022 Jul - 2024 Sep"
            }
    }

    # Check fetch_relation was called correctly
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId='gov_01',
        relation=Relation(name='AS_PRIME_MINISTER', activeAt=Util.normalize_timestamp(selected_date), direction='OUTGOING')
    )

@pytest.mark.asyncio 
async def test_prime_minister_without_person_data(organisation_service, mock_opengin_service):
    selected_date = "2021-10-27"

    mock_response = Relation(name='AS_PRIME_MINISTER', activeAt='', relatedEntityId='cit_3', startTime='2022-07-26T00:00:00Z', endTime='2024-09-23T00:00:00Z', id='person_123', direction='OUTGOING')
    mock_opengin_service.fetch_relation.return_value = [mock_response]

    # Patch enrich_department_item with AsyncMock returning the department dict
    with patch(
        "services.organisation_service.OrganisationService.enrich_person_data",
        new_callable=AsyncMock
    ) as mock_enrich_person:
        mock_enrich_person.return_value = {}

        result = await organisation_service.fetch_prime_minister(selected_date=selected_date)

    assert result == {"body": {}}

    # Check fetch_relation was called correctly
    mock_opengin_service.fetch_relation.assert_called_with(
        entityId='gov_01',
        relation=Relation(name='AS_PRIME_MINISTER', activeAt=Util.normalize_timestamp(selected_date), direction='OUTGOING')
    )

@pytest.mark.asyncio 
async def test_prime_minister_without_selected_date(organisation_service):
    selected_date = None

    with pytest.raises(BadRequestError):
        await organisation_service.fetch_prime_minister(
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_prime_minister_with_empty_selected_date(organisation_service):
    selected_date = ""

    with pytest.raises(BadRequestError):
        await organisation_service.fetch_prime_minister(
            selected_date=selected_date
        )

@pytest.mark.asyncio 
async def test_prime_minister_with_no_relation(organisation_service, mock_opengin_service):
    selected_date = "2021-10-27"

    mock_opengin_service.fetch_relation.return_value = []

    result = await organisation_service.fetch_prime_minister(selected_date=selected_date)
    assert result == {"body": {}}

@pytest.mark.asyncio 
async def test_prime_minister_with_internal_server_error(organisation_service, mock_opengin_service):
    selected_date = "2021-10-27"
    original_error_message = "OpenGIN service error"

    mock_opengin_service.fetch_relation.side_effect = Exception(original_error_message)

    with pytest.raises(InternalServerError) as exc_info:
        await organisation_service.fetch_prime_minister(selected_date=selected_date)
    
    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == original_error_message

@pytest.mark.asyncio
async def test_department_history_timeline_success(organisation_service, mock_opengin_service):
    # Setup IDs
    # Lineage: dep_01 -> dep_02 (via RENAMED_TO)
    department_id = "dep_01"
    
    # Mock _get_renamed_lineage
    # 1. dep_01 -> fetch_relation(RENAMED_TO) -> [dep_02]
    # 2. dep_02 -> fetch_relation(RENAMED_TO) -> []
    
    # Mock _fetch_and_map_relations (AS_DEPARTMENT)
    # 1. dep_01 -> [min_01 (2020-01 to 2021-01)]
    # 2. dep_02 -> [min_02 (2021-01 to 2022-01)]
    
    # Mock _fetch_and_map_entities (Ministries)
    # min_01, min_02
    
    # Mock _fetch_and_map_relations (AS_APPOINTED)
    # min_01 -> [pers_01 (2020-02 to 2020-08)]
    # min_02 -> [pers_01 (2021-05 to 2021-12)]
    
    # Mock _fetch_and_map_entities (Persons)
    # pers_01
    
    # Mock President context
    # gov_01 -> fetch_relation(AS_PRESIDENT) -> [pres_01 (Open-ended)]
    # pres_01 -> get_entities -> President Entity
    
    async def fetch_relation_handler(entityId, relation):
        if relation.name == "RENAMED_TO":
            return [Relation(relatedEntityId="dep_02")] if entityId == "dep_01" else []
        if relation.name == "AS_DEPARTMENT":
            if entityId == "dep_01":
                return [Relation(relatedEntityId="min_01", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")]
            if entityId == "dep_02":
                return [Relation(relatedEntityId="min_02", startTime="2021-01-01T00:00:00Z", endTime="2022-01-01T00:00:00Z")]
        if relation.name == "AS_APPOINTED":
            if entityId == "min_01":
                return [Relation(relatedEntityId="pers_01", startTime="2020-02-01T00:00:00Z", endTime="2020-08-01T00:00:00Z")]
            if entityId == "min_02":
                return [Relation(relatedEntityId="pers_01", startTime="2021-05-01T00:00:00Z", endTime="2021-12-01T00:00:00Z")]
        if entityId == "gov_01" and relation.name == "AS_PRESIDENT":
            return [Relation(relatedEntityId="pres_01", startTime="2019-01-01T00:00:00Z", endTime="")]
        return []

    async def get_entities_handler(entity):
        mapping = {
            "min_01": "4d696e6973747279204f6e65",
            "min_02": "4d696e69737472792054776f",
            "pers_01": "4d696e69737465722041",
            "pres_01": "507265736964656e742058"
        }
        name_hex = mapping.get(entity.id)
        return [Entity(id=entity.id, name=f'{{"value": "{name_hex}"}}')] if name_hex else []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler
    mock_opengin_service.get_entities.side_effect = get_entities_handler

    result = await organisation_service.department_history_timeline(department_id=department_id)
    
    assert result is not None
    assert isinstance(result, list)
    
    # We expect:
    # 1. 2021-12-01 to 2022-01-01: Ministry Two - Gap (filled by President X)
    # 2. 2021-05-01 to 2021-12-01: Ministry Two - Minister A
    # 3. 2021-01-01 to 2021-05-01: Ministry Two - Gap (filled by President X)
    # 4. 2020-08-01 to 2021-01-01: Ministry One - Gap (filled by President X)
    # 5. 2020-02-01 to 2020-08-01: Ministry One - Minister A
    # 6. 2020-01-01 to 2020-02-01: Ministry One - Gap (filled by President X)

    # Note: Sequential entries with SAME MINISTER and SAME MINISTRY NAME are collapsed.
    # In this test, min_01 and min_02 have different names ("Ministry One" vs "Ministry Two"), 
    # so Minister A won't collapse across them.
    
    assert len(result) == 6
    assert result[0]["minister_name"] == "President X"
    assert result[1]["minister_name"] == "Minister A"
    assert result[1]["ministry_name"] == "Ministry Two"
    assert result[4]["minister_name"] == "Minister A"
    assert result[4]["ministry_name"] == "Ministry One"
    assert "period" in result[0]
    assert "startTime" not in result[0]
    assert "endTime" not in result[0]

@pytest.mark.asyncio
async def test_department_history_timeline_collapsing(organisation_service, mock_opengin_service):
    # Setup IDs: Same ministry name ("Ministry of Media") for two different ministry IDs
    department_id = "dep_01"
    
    async def fetch_relation_handler(entityId, relation):
        if relation.name == "RENAMED_TO":
            return []
        if relation.name == "AS_DEPARTMENT":
            return [
                Relation(relatedEntityId="min_01", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z"),
                Relation(relatedEntityId="min_02", startTime="2021-01-01T00:00:00Z", endTime="2022-01-01T00:00:00Z")
            ]
        if relation.name == "AS_APPOINTED":
            if entityId == "min_01":
                return [Relation(relatedEntityId="pers_01", startTime="2020-01-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")]
            if entityId == "min_02":
                return [Relation(relatedEntityId="pers_01", startTime="2021-01-01T00:00:00Z", endTime="2022-01-01T00:00:00Z")]
        return []

    async def get_entities_handler(entity):
        if entity.id in ["min_01", "min_02"]:
            # "Ministry of Media" in hex
            return [Entity(id=entity.id, name='{"value": "4d696e6973747279206f66204d65646961"}')]
        if entity.id == "pers_01":
            # "Ranil" in hex
            return [Entity(id="pers_01", name='{"value": "52616e696c"}')]
        return []

    mock_opengin_service.fetch_relation.side_effect = fetch_relation_handler
    mock_opengin_service.get_entities.side_effect = get_entities_handler

    result = await organisation_service.department_history_timeline(department_id=department_id)
    
    # Should collapse into ONE entry because same name and same person across min_01 and min_02
    assert len(result) == 1
    assert result[0]["minister_name"] == "Ranil"
    assert result[0]["period"] == "2020-01-01 - 2022-01-01"

@pytest.mark.asyncio
async def test_get_renamed_lineage_chain(organisation_service, mock_opengin_service):
    # Chain: A -> B -> C
    start_id = "A"
    mock_opengin_service.fetch_relation.side_effect = [
        [Relation(relatedEntityId="B")], # A -> B
        [Relation(relatedEntityId="C")], # B -> C
        []                              # C -> none
    ]
    
    result = await organisation_service._get_renamed_lineage(start_id)
    assert result == {"A", "B", "C"}
    assert mock_opengin_service.fetch_relation.call_count == 3

@pytest.mark.asyncio
async def test_get_renamed_lineage_no_renaming(organisation_service, mock_opengin_service):
    start_id = "A"
    mock_opengin_service.fetch_relation.return_value = []
    
    result = await organisation_service._get_renamed_lineage(start_id)
    assert result == {"A"}
    mock_opengin_service.fetch_relation.assert_called_once()

@pytest.mark.asyncio
async def test_fetch_and_map_entities_success(organisation_service, mock_opengin_service):
    entity_ids = ["e1", "e2"]
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="e1", name="name1")],
        [Entity(id="e2", name="name2")]
    ]
    
    result = await organisation_service._fetch_and_map_entities(entity_ids)
    
    assert len(result) == 2
    assert result["e1"].id == "e1"
    assert result["e2"].id == "e2"
    assert mock_opengin_service.get_entities.call_count == 2

@pytest.mark.asyncio
async def test_fetch_and_map_entities_partial_failure(organisation_service, mock_opengin_service):
    entity_ids = ["e1", "e2"]
    # Suppose e2 fails or returns nothing
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="e1", name="name1")],
        Exception("Failed to fetch")
    ]
    
    result = await organisation_service._fetch_and_map_entities(entity_ids)
    
    assert len(result) == 1
    assert "e1" in result
    assert "e2" not in result

@pytest.mark.asyncio
async def test_fetch_and_map_relations_success(organisation_service, mock_opengin_service):
    entity_ids = ["e1", "e2"]
    query = Relation(name="TEST")
    r1 = Relation(relatedEntityId="r1")
    r2 = Relation(relatedEntityId="r2")
    
    mock_opengin_service.fetch_relation.side_effect = [
        [r1],
        [r2]
    ]
    
    result = await organisation_service._fetch_and_map_relations(entity_ids, query)
    
    assert len(result) == 2
    assert result["e1"] == [r1]
    assert result["e2"] == [r2]

@pytest.mark.asyncio
async def test_fetch_and_map_relations_with_errors(organisation_service, mock_opengin_service):
    entity_ids = ["e1", "e2"]
    query = Relation(name="TEST")
    
    mock_opengin_service.fetch_relation.side_effect = [
        [Relation(relatedEntityId="r1")],
        Exception("Error")
    ]
    
    result = await organisation_service._fetch_and_map_relations(entity_ids, query)
    
    assert len(result) == 2
    assert len(result["e1"]) == 1
    assert result["e2"] == [] # Should default to empty list on error