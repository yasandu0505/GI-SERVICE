import pytest
from unittest.mock import patch, AsyncMock
from src.exception.exceptions import InternalServerError, BadRequestError
from src.models.organisation_schemas import Entity, Kind
from src.models.search_schemas import SearchResult, SearchResponse
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


# ============ Tests for search_departments ============

@pytest.mark.asyncio
async def test_search_departments_success(search_service, mock_opengin_service):
    """Test search_departments returns matching departments"""

    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_health_dept",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z",
            terminated=""
        ),
        Entity(
            id="dept_2",
            name="encoded_education_dept",
            kind=Kind(major="Organisation", minor="department"),
            created="2019-06-15T00:00:00Z",
            terminated=""
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_departments

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Ministry of Health", "Ministry of Education"]
    ):
        result = await search_service.search_departments(
            query="health",
            as_of_date="2022-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "dept_1"
    assert result[0]["name"] == "Ministry of Health"
    assert result[0]["type"] == "department"
    assert result[0]["match_score"] == 0.6


@pytest.mark.asyncio
async def test_search_departments_filters_by_year(search_service, mock_opengin_service):
    """Test search_departments filters entities by created year"""

    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_old_dept",
            kind=Kind(major="Organisation", minor="department"),
            created="2015-01-01T00:00:00Z"
        ),
        Entity(
            id="dept_2",
            name="encoded_new_dept",
            kind=Kind(major="Organisation", minor="department"),
            created="2023-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_departments

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Health Department Old", "Health Department New"]
    ):
        result = await search_service.search_departments(
            query="health",
            as_of_date="2020-01-01",  # Only entities created before 2020 should match
            limit=10
        )

    # Only the old department should be returned
    assert len(result) == 1
    assert result[0]["id"] == "dept_1"


@pytest.mark.asyncio
async def test_search_departments_empty_result(search_service, mock_opengin_service):
    """Test search_departments returns empty list when no matches"""

    mock_departments = [
        Entity(
            id="dept_1",
            name="encoded_education",
            kind=Kind(major="Organisation", minor="department"),
            created="2020-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_departments

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Ministry of Education"
    ):
        result = await search_service.search_departments(
            query="health",
            as_of_date="2022-01-01",
            limit=10
        )

    assert len(result) == 0


@pytest.mark.asyncio
async def test_search_departments_handles_error(search_service, mock_opengin_service):
    """Test search_departments returns empty list on error"""

    mock_opengin_service.get_entities.side_effect = Exception("Database error")

    result = await search_service.search_departments(
        query="health",
        as_of_date="2022-01-01",
        limit=10
    )

    assert len(result) == 0


# ============ Tests for search_ministers ============

@pytest.mark.asyncio
async def test_search_ministers_success(search_service, mock_opengin_service):
    """Test search_ministers returns matching ministers"""

    mock_ministers = [
        Entity(
            id="minister_1",
            name="encoded_health_minister",
            kind=Kind(major="Organisation", minor="minister"),
            created="2020-01-01T00:00:00Z",
            terminated=""
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_ministers

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Health Minister"
    ):
        result = await search_service.search_ministers(
            query="health",
            as_of_date="2022-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "minister_1"
    assert result[0]["name"] == "Health Minister"
    assert result[0]["type"] == "minister"


@pytest.mark.asyncio
async def test_search_ministers_filters_by_year(search_service, mock_opengin_service):
    """Test search_ministers filters entities by created year"""

    mock_ministers = [
        Entity(
            id="minister_1",
            name="encoded_old_minister",
            kind=Kind(major="Organisation", minor="minister"),
            created="2018-01-01T00:00:00Z"
        ),
        Entity(
            id="minister_2",
            name="encoded_future_minister",
            kind=Kind(major="Organisation", minor="minister"),
            created="2025-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_ministers

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Health Portfolio", "Health Future Portfolio"]
    ):
        result = await search_service.search_ministers(
            query="health",
            as_of_date="2022-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "minister_1"


@pytest.mark.asyncio
async def test_search_ministers_handles_error(search_service, mock_opengin_service):
    """Test search_ministers returns empty list on error"""

    mock_opengin_service.get_entities.side_effect = Exception("Service unavailable")

    result = await search_service.search_ministers(
        query="health",
        as_of_date="2022-01-01",
        limit=10
    )

    assert len(result) == 0


# ============ Tests for search_datasets ============

@pytest.mark.asyncio
async def test_search_datasets_success(search_service, mock_opengin_service):
    """Test search_datasets returns matching datasets"""

    mock_datasets = [
        Entity(
            id="dataset_1",
            name="encoded_health_stats",
            kind=Kind(major="Dataset", minor="tabular"),
            created="2021-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_datasets

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="Health Statistics-2021"
    ):
        result = await search_service.search_datasets(
            query="health",
            as_of_date="2022-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "dataset_1"
    assert result[0]["type"] == "dataset"
    assert result[0]["year"] == "2021"


@pytest.mark.asyncio
async def test_search_datasets_filters_by_year(search_service, mock_opengin_service):
    """Test search_datasets filters by created year"""

    mock_datasets = [
        Entity(
            id="dataset_1",
            name="encoded_old_data",
            kind=Kind(major="Dataset", minor="tabular"),
            created="2019-01-01T00:00:00Z"
        ),
        Entity(
            id="dataset_2",
            name="encoded_new_data",
            kind=Kind(major="Dataset", minor="tabular"),
            created="2023-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_datasets

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["Health Data 2019", "Health Data 2023"]
    ):
        result = await search_service.search_datasets(
            query="health",
            as_of_date="2020-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "dataset_1"


@pytest.mark.asyncio
async def test_search_datasets_handles_error(search_service, mock_opengin_service):
    """Test search_datasets returns empty list on error"""

    mock_opengin_service.get_entities.side_effect = Exception("Database timeout")

    result = await search_service.search_datasets(
        query="health",
        as_of_date="2022-01-01",
        limit=10
    )

    assert len(result) == 0


# ============ Tests for search_persons ============

@pytest.mark.asyncio
async def test_search_persons_success(search_service, mock_opengin_service):
    """Test search_persons returns matching persons"""

    mock_persons = [
        Entity(
            id="person_1",
            name="encoded_person_name",
            kind=Kind(major="Person", minor="citizen"),
            created="2020-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_persons

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        return_value="John Smith"
    ):
        result = await search_service.search_persons(
            query="john",
            as_of_date="2022-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "person_1"
    assert result[0]["name"] == "John Smith"
    assert result[0]["type"] == "person"


@pytest.mark.asyncio
async def test_search_persons_filters_by_year(search_service, mock_opengin_service):
    """Test search_persons filters by created year"""

    mock_persons = [
        Entity(
            id="person_1",
            name="encoded_old_person",
            kind=Kind(major="Person", minor="citizen"),
            created="2015-01-01T00:00:00Z"
        ),
        Entity(
            id="person_2",
            name="encoded_new_person",
            kind=Kind(major="Person", minor="citizen"),
            created="2023-01-01T00:00:00Z"
        )
    ]

    mock_opengin_service.get_entities.return_value = mock_persons

    with patch(
        "src.services.search_service.Util.decode_protobuf_attribute_name",
        side_effect=["John Old", "John New"]
    ):
        result = await search_service.search_persons(
            query="john",
            as_of_date="2020-01-01",
            limit=10
        )

    assert len(result) == 1
    assert result[0]["id"] == "person_1"


@pytest.mark.asyncio
async def test_search_persons_handles_error(search_service, mock_opengin_service):
    """Test search_persons returns empty list on error"""

    mock_opengin_service.get_entities.side_effect = Exception("Connection refused")

    result = await search_service.search_persons(
        query="john",
        as_of_date="2022-01-01",
        limit=10
    )

    assert len(result) == 0


# ============ Tests for helper methods ============

@pytest.mark.asyncio
async def test_calculate_match_score_exact_match(search_service):
    """Test _calculate_match_score returns 1.0 for exact match"""
    score = search_service._calculate_match_score("health", "health")
    assert score == 1.0


@pytest.mark.asyncio
async def test_calculate_match_score_starts_with(search_service):
    """Test _calculate_match_score returns 0.8 for starts with match"""
    score = search_service._calculate_match_score("health", "Health Ministry")
    assert score == 0.8


@pytest.mark.asyncio
async def test_calculate_match_score_contains(search_service):
    """Test _calculate_match_score returns 0.6 for contains match"""
    score = search_service._calculate_match_score("health", "Ministry of Health")
    assert score == 0.6


@pytest.mark.asyncio
async def test_calculate_match_score_no_match(search_service):
    """Test _calculate_match_score returns 0.0 for no match"""
    score = search_service._calculate_match_score("health", "Education Department")
    assert score == 0.0


@pytest.mark.asyncio
async def test_calculate_match_score_empty_text(search_service):
    """Test _calculate_match_score returns 0.0 for empty text"""
    score = search_service._calculate_match_score("health", "")
    assert score == 0.0


@pytest.mark.asyncio
async def test_calculate_match_score_none_text(search_service):
    """Test _calculate_match_score returns 0.0 for None text"""
    score = search_service._calculate_match_score("health", None)
    assert score == 0.0


@pytest.mark.asyncio
async def test_matches_query_true(search_service):
    """Test _matches_query returns True for matching text"""
    assert search_service._matches_query("health", "Ministry of Health") is True


@pytest.mark.asyncio
async def test_matches_query_false(search_service):
    """Test _matches_query returns False for non-matching text"""
    assert search_service._matches_query("health", "Education Department") is False


@pytest.mark.asyncio
async def test_matches_query_case_insensitive(search_service):
    """Test _matches_query is case insensitive"""
    assert search_service._matches_query("HEALTH", "ministry of health") is True
    assert search_service._matches_query("health", "MINISTRY OF HEALTH") is True


@pytest.mark.asyncio
async def test_matches_query_empty_text(search_service):
    """Test _matches_query returns False for empty text"""
    assert search_service._matches_query("health", "") is False


@pytest.mark.asyncio
async def test_matches_query_empty_query(search_service):
    """Test _matches_query returns False for empty query"""
    assert search_service._matches_query("", "Ministry of Health") is False


@pytest.mark.asyncio
async def test_extract_year_valid_date(search_service):
    """Test _extract_year returns correct year from date string"""
    assert search_service._extract_year("2022-01-15T00:00:00Z") == 2022
    assert search_service._extract_year("2020-12-31") == 2020


@pytest.mark.asyncio
async def test_extract_year_empty_string(search_service):
    """Test _extract_year returns 0 for empty string"""
    assert search_service._extract_year("") == 0


@pytest.mark.asyncio
async def test_extract_year_none(search_service):
    """Test _extract_year returns 0 for None"""
    assert search_service._extract_year(None) == 0


@pytest.mark.asyncio
async def test_extract_year_invalid_format(search_service):
    """Test _extract_year returns 0 for invalid format"""
    assert search_service._extract_year("invalid-date") == 0
