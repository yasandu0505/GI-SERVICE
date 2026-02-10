import pytest
from unittest.mock import patch, AsyncMock
from src.exception.exceptions import BadRequestError
from src.models.organisation_schemas import Entity, Kind
from src.models.search_schemas import SearchResponse
from src.services.search_service import SearchService
from src.services.opengin_service import OpenGINService


# Fixtures
@pytest.fixture
def mock_opengin_service():
    """Mock OpenGINService for testing"""
    return AsyncMock(spec=OpenGINService)


@pytest.fixture
def search_service(mock_opengin_service):
    """SearchService fixture with mocked OpenGINService"""
    config = {}
    return SearchService(config, mock_opengin_service)


# ============ Tests for unified_search ============

@pytest.mark.asyncio
async def test_unified_search_success(search_service, mock_opengin_service):
    """Test unified_search returns results from all entity types"""

    # Mock departments
    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_health",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        )
    ]

    # Mock ministers
    mock_ministers = [
        Entity(
            id="minister_1",
            name="encoded_minister",
            kind=Kind(major="Organisation", minor="minister"),
            created="2019-06-15T00:00:00Z"
        )
    ]

    # Mock datasets
    mock_datasets = [
        Entity(
            id="dataset_1",
            name="encoded_dataset",
            kind=Kind(major="Dataset", minor="tabular"),
            created="2021-03-20T00:00:00Z"
        )
    ]

    # Mock persons
    mock_persons = [
        Entity(
            id="person_1",
            name="encoded_person",
            kind=Kind(major="Person", minor="citizen"),
            created="2018-12-01T00:00:00Z"
        )
    ]

    # Setup mock to return different entities based on Kind
    def get_entities_side_effect(entity):
        if entity.kind.minor == "department":
            return mock_departments
        elif entity.kind.minor == "minister":
            return mock_ministers
        elif entity.kind.minor == "tabular":
            return mock_datasets
        elif entity.kind.minor == "citizen":
            return mock_persons
        return []

    mock_opengin_service.get_entities.side_effect = get_entities_side_effect

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Ministry of Health", "Health Minister", "Health Statistics", "John Health"]
    ):
        result = await search_service.unified_search(
            query="health",
            as_of_date="2022-01-01",
            limit=20
        )

    assert isinstance(result, SearchResponse)
    assert result.query == "health"
    assert result.as_of_date == "2022-01-01"
    assert result.total == 4
    assert len(result.results) == 4

    # Verify all types are present
    types = [r.type for r in result.results]
    assert "department" in types
    assert "minister" in types
    assert "dataset" in types
    assert "person" in types


@pytest.mark.asyncio
async def test_unified_search_empty_query(search_service):
    """Test unified_search raises BadRequestError for empty query"""
    with pytest.raises(BadRequestError) as exc_info:
        await search_service.unified_search(
            query="",
            as_of_date="2022-01-01",
            limit=20
        )

    assert "at least 2 characters" in str(exc_info.value)


@pytest.mark.asyncio
async def test_unified_search_short_query(search_service):
    """Test unified_search raises BadRequestError for query less than 2 chars"""
    with pytest.raises(BadRequestError) as exc_info:
        await search_service.unified_search(
            query="a",
            as_of_date="2022-01-01",
            limit=20
        )

    assert "at least 2 characters" in str(exc_info.value)


@pytest.mark.asyncio
async def test_unified_search_none_query(search_service):
    """Test unified_search raises BadRequestError for None query"""
    with pytest.raises(BadRequestError) as exc_info:
        await search_service.unified_search(
            query=None,
            as_of_date="2022-01-01",
            limit=20
        )

    assert "at least 2 characters" in str(exc_info.value)


@pytest.mark.asyncio
async def test_unified_search_none_date(search_service):
    """Test unified_search raises BadRequestError for None date"""
    with pytest.raises(BadRequestError) as exc_info:
        await search_service.unified_search(
            query="health",
            as_of_date=None,
            limit=20
        )

    assert "date is required" in str(exc_info.value)


@pytest.mark.asyncio
async def test_unified_search_no_results(search_service, mock_opengin_service):
    """Test unified_search returns empty results when no matches found"""
    mock_opengin_service.get_entities.return_value = []

    result = await search_service.unified_search(
        query="xyz123",
        as_of_date="2022-01-01",
        limit=20
    )

    assert result.total == 0
    assert len(result.results) == 0


@pytest.mark.asyncio
async def test_unified_search_respects_limit(search_service, mock_opengin_service):
    """Test unified_search respects the limit parameter"""

    # Create many mock entities
    mock_entities = [
        Entity(
            id=f"dept_{i}",
            name=f"encoded_dept_{i}",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        )
        for i in range(10)
    ]

    mock_opengin_service.get_entities.return_value = mock_entities

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=[f"Health Department {i}" for i in range(40)]
    ):
        result = await search_service.unified_search(
            query="health",
            as_of_date="2022-01-01",
            limit=5
        )

    assert result.total == 5
    assert len(result.results) == 5


@pytest.mark.asyncio
async def test_unified_search_sorted_by_score(search_service, mock_opengin_service):
    """Test unified_search results are sorted by match score (highest first)"""

    # Only return departments, empty for other types
    mock_departments = [
        Entity(id="dept_1", name="encoded_1", kind=Kind(major="Organisation", minor="department"), created="2020-01-01T00:00:00Z"),
        Entity(id="dept_2", name="encoded_2", kind=Kind(major="Organisation", minor="department"), created="2020-01-01T00:00:00Z"),
        Entity(id="dept_3", name="encoded_3", kind=Kind(major="Organisation", minor="department"), created="2020-01-01T00:00:00Z"),
    ]

    def get_entities_side_effect(entity):
        if entity.kind.minor == "department":
            return mock_departments
        return []  # Empty for other types

    mock_opengin_service.get_entities.side_effect = get_entities_side_effect

    # Return names with different match scores:
    # "Ministry of Health" -> contains (0.6)
    # "Health Ministry" -> starts with (0.8)
    # "health" -> exact match (1.0)
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Ministry of Health", "Health Ministry", "health"]
    ):
        result = await search_service.unified_search(
            query="health",
            as_of_date="2022-01-01",
            limit=20
        )

    # Results should be sorted by score: exact match first
    assert len(result.results) == 3
    assert result.results[0].match_score == 1.0  # "health" exact match
    assert result.results[1].match_score == 0.8  # "Health Ministry" starts with
    assert result.results[2].match_score == 0.6  # "Ministry of Health" contains


@pytest.mark.asyncio
async def test_unified_search_handles_partial_failures(search_service, mock_opengin_service):
    """Test unified_search continues when some searches fail"""

    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_health",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        )
    ]

    # Make get_entities return departments for department kind, raise for others
    def get_entities_side_effect(entity):
        if entity.kind.minor == "department":
            return mock_departments
        raise Exception("Service unavailable")

    mock_opengin_service.get_entities.side_effect = get_entities_side_effect

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Ministry of Health"
    ):
        result = await search_service.unified_search(
            query="health",
            as_of_date="2022-01-01",
            limit=20
        )

    # Should still return department results even though other searches failed
    assert result.total == 1
    assert result.results[0].type == "department"

#tests for entity_specific_search

@pytest.mark.asyncio
async def test_entity_specific_search_departments(search_service, mock_opengin_service):
    """Test entity_specific_search returns correct results for departments"""
    
    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_health",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z",
            terminated=""
        ),
        Entity(
            id="dept_2",
            name="encoded_education",
            kind=Kind(major="Organisation", minor="department"),
            created="2019-06-15T00:00:00Z",
            terminated="2021-12-31T00:00:00Z"
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_departments
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Ministry of Health", "Ministry of Education"]
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="department",
            query="Ministry",
            as_of_date="2022-01-01"
        )
    
    assert len(results) == 2
    assert results[0]["type"] == "department"
    assert results[0]["id"] == "dept_1"
    assert results[0]["name"] == "Ministry of Health"
    assert results[0]["created"] == "2020-01-01T00:00:00Z"
    assert results[0]["terminated"] == ""
    
    assert results[1]["type"] == "department"
    assert results[1]["id"] == "dept_2"
    assert results[1]["name"] == "Ministry of Education"
    assert results[1]["terminated"] == "2021-12-31T00:00:00Z"

@pytest.mark.asyncio
async def test_entity_specific_search_ministers(search_service, mock_opengin_service):
    """Test entity_specific_search returns correct results for ministers"""
    
    mock_ministers = [
        Entity(
            id="minister_1",
            name="encoded_minister",
            kind=Kind(major="Organisation", minor="minister"),
            created="2019-06-15T00:00:00Z",
            terminated=""
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_ministers
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Health Minister"
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="minister",
            query="Health",
            as_of_date="2022-01-01"
        )
    
    assert len(results) == 1
    assert results[0]["type"] == "minister"
    assert results[0]["id"] == "minister_1"
    assert results[0]["name"] == "Health Minister"
    assert results[0]["created"] == "2019-06-15T00:00:00Z"

@pytest.mark.asyncio
async def test_entity_specific_search_datasets(search_service, mock_opengin_service):
    """Test entity_specific_search returns correct results for datasets"""
    
    mock_datasets = [
        Entity(
            id="dataset_1",
            name="encoded_dataset",
            kind=Kind(major="Dataset", minor="tabular"),
            created="2021-03-20T00:00:00Z",
            terminated=""
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_datasets
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="health_statistics_2021"
    ), patch(
        "src.services.search_service.Util.get_name_without_year",
        return_value="health_statistics"
    ), patch(
        "src.services.search_service.Util.to_title_case",
        return_value="Health Statistics"
    ):
        results = await search_service.entity_specific_search(
            major="Dataset",
            minor="tabular",
            query="health",
            as_of_date="2022-01-01"
        )
    
    assert len(results) == 1
    assert results[0]["type"] == "dataset"
    assert results[0]["id"] == "dataset_1"
    assert results[0]["name"] == "Health Statistics"
    assert results[0]["created"] == "2021-03-20T00:00:00Z"

@pytest.mark.asyncio
async def test_entity_specific_search_persons(search_service, mock_opengin_service):
    """Test entity_specific_search returns correct results for persons"""
    
    mock_persons = [
        Entity(
            id="person_1",
            name="encoded_person",
            kind=Kind(major="Person", minor="citizen"),
            created="2018-12-01T00:00:00Z",
            terminated=""
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_persons
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="John Doe"
    ):
        results = await search_service.entity_specific_search(
            major="Person",
            minor="citizen",
            query="John",
            as_of_date="2022-01-01"
        )
    
    assert len(results) == 1
    assert results[0]["type"] == "person"
    assert results[0]["id"] == "person_1"
    assert results[0]["name"] == "John Doe"
    assert results[0]["created"] == "2018-12-01T00:00:00Z"

@pytest.mark.asyncio
async def test_entity_specific_search_filters_by_date(search_service, mock_opengin_service):
    """Test entity_specific_search filters out entities created after as_of_date"""
    
    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_1",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        ),
        Entity(
            id="dept_2",
            name="encoded_2",
            kind=Kind(major="Organisation", minor="department"),
            created="2023-01-01T00:00:00Z"  # Created after as_of_date
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_departments
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Health Department", "Future Department"]
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="department",
            query="Department",
            as_of_date="2022-01-01"
        )
    
    # Only the entity created before 2022 should be returned
    assert len(results) == 1
    assert results[0]["id"] == "dept_1"

@pytest.mark.asyncio
async def test_entity_specific_search_sorted_by_score(search_service, mock_opengin_service):
    """Test entity_specific_search results are sorted by match score"""
    
    mock_departments = [
        Entity(id="dept_1", name="encoded_1", kind=Kind(major="Organisation", minor="department"), created="2020-01-01T00:00:00Z"),
        Entity(id="dept_2", name="encoded_2", kind=Kind(major="Organisation", minor="department"), created="2020-01-01T00:00:00Z"),
        Entity(id="dept_3", name="encoded_3", kind=Kind(major="Organisation", minor="department"), created="2020-01-01T00:00:00Z"),
    ]
    
    mock_opengin_service.get_entities.return_value = mock_departments
    
    # Return names with different match scores
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Ministry of Health", "Health Ministry", "health"]
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="department",
            query="health",
            as_of_date="2022-01-01"
        )
    
    # Results should be sorted by score: exact match first
    assert len(results) == 3
    assert results[0]["match_score"] == 1.0  # "health" exact match
    assert results[1]["match_score"] == 0.8  # "Health Ministry" starts with
    assert results[2]["match_score"] == 0.6  # "Ministry of Health" contains

@pytest.mark.asyncio
async def test_entity_specific_search_respects_limit(search_service, mock_opengin_service):
    """Test entity_specific_search respects the limit parameter"""
    
    # Create 10 mock departments
    mock_departments = [
        Entity(
            id=f"dept_{i}",
            name=f"encoded_{i}",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        )
        for i in range(10)
    ]
    
    mock_opengin_service.get_entities.return_value = mock_departments
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=[f"Health Department {i}" for i in range(10)]
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="department",
            query="Health",
            as_of_date="2022-01-01",
            limit=3
        )
    
    assert len(results) == 3

@pytest.mark.asyncio
async def test_entity_specific_search_handles_exceptions(search_service, mock_opengin_service):
    """Test entity_specific_search returns empty list on exception"""
    
    # Make get_entities raise an exception
    mock_opengin_service.get_entities.side_effect = Exception("Service error")
    
    results = await search_service.entity_specific_search(
        major="Organisation",
        minor="department",
        query="health",
        as_of_date="2022-01-01"
    )
    
    assert results == []

@pytest.mark.asyncio
async def test_entity_specific_search_case_insensitive(search_service, mock_opengin_service):
    """Test entity_specific_search is case insensitive"""
    
    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_health",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_departments
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Ministry of Health"
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="department",
            query="HEALTH",
            as_of_date="2022-01-01"
        )
    
    assert len(results) == 1
    assert results[0]["name"] == "Ministry of Health"

@pytest.mark.asyncio
async def test_entity_specific_search_handles_entities_without_created(search_service, mock_opengin_service):
    """Test entity_specific_search filters out entities without created date (defaults to year 9999)"""
    
    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_health",
            kind=Kind(major="Organisation", minor="department"),
            created=""  # Empty created date (will default to year 9999 in extract_year)
        )
    ]
    
    mock_opengin_service.get_entities.return_value = mock_departments
    
    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Ministry of Health"
    ):
        results = await search_service.entity_specific_search(
            major="Organisation",
            minor="department",
            query="Health",
            as_of_date="2022-01-01"
        )
    
    # Entity without created date defaults to year 9999, which is after 2022, so it should be filtered out
    assert len(results) == 0


#tests for _determine_entity_type

def test_determine_entity_type_department(search_service):
    """Test _determine_entity_type returns 'department' for Organisation/department"""
    entity_type = search_service._determine_entity_type("Organisation", "department")
    assert entity_type == "department"


def test_determine_entity_type_minister(search_service):
    """Test _determine_entity_type returns 'minister' for Organisation/minister"""
    entity_type = search_service._determine_entity_type("Organisation", "minister")
    assert entity_type == "minister"


def test_determine_entity_type_dataset(search_service):
    """Test _determine_entity_type returns 'dataset' for Dataset/tabular"""
    entity_type = search_service._determine_entity_type("Dataset", "tabular")
    assert entity_type == "dataset"


def test_determine_entity_type_person(search_service):
    """Test _determine_entity_type returns 'person' for Person/citizen"""
    entity_type = search_service._determine_entity_type("Person", "citizen")
    assert entity_type == "person"


def test_determine_entity_type_case_insensitive(search_service):
    """Test _determine_entity_type is case insensitive"""
    assert search_service._determine_entity_type("organisation", "department") == "department"
    assert search_service._determine_entity_type("ORGANISATION", "MINISTER") == "minister"
    assert search_service._determine_entity_type("dataset", "TABULAR") == "dataset"
    assert search_service._determine_entity_type("PERSON", "citizen") == "person"


def test_determine_entity_type_unknown_combination(search_service):
    """Test _determine_entity_type returns 'unknown' for unknown major/minor combination"""
    entity_type = search_service._determine_entity_type("Unknown", "type")
    assert entity_type == "unknown"


def test_determine_entity_type_empty_major(search_service):
    """Test _determine_entity_type returns 'unknown' for empty major"""
    entity_type = search_service._determine_entity_type("", "department")
    assert entity_type == "unknown"


def test_determine_entity_type_empty_minor(search_service):
    """Test _determine_entity_type returns 'unknown' for empty minor"""
    entity_type = search_service._determine_entity_type("Organisation", "")
    assert entity_type == "unknown"


def test_determine_entity_type_partial_match(search_service):
    """Test _determine_entity_type doesn't match partial combinations"""
    # Valid major, but invalid minor
    entity_type = search_service._determine_entity_type("Organisation", "invalid")
    assert entity_type == "unknown"
    
    # Valid minor, but invalid major
    entity_type = search_service._determine_entity_type("Invalid", "department")
    assert entity_type == "unknown"
