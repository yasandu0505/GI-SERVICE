import pytest
import asyncio
from src.exception.exceptions import InternalServerError, BadRequestError, NotFoundError
from unittest.mock import patch
from src.models.organisation_schemas import Entity, Relation, Kind, Dataset, Category

# Tests for enrich_dataset
@pytest.mark.asyncio
async def test_enrich_dataset_with_dataset_entity(data_service, mock_opengin_service):
    """Test enrich_dataset with a dataset entity"""
    dataset = Entity(id="dataset_123", name="population-2020", kind=Kind(major="Dataset", minor="tabular"))
    dataset_dictionary = {}
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="Population-2020"
    ):
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            dataset=dataset
        )
    
    # After removing year (-2020), we get "Population", then title case
    assert "Population" in dataset_dictionary

@pytest.mark.asyncio
async def test_enrich_dataset_with_dataset_relation(data_service, mock_opengin_service):
    """Test enrich_dataset with a dataset relation"""
    dataset_relation = Relation(
        relatedEntityId="dataset_456",
        name="IS_ATTRIBUTE",
        direction="OUTGOING"
    )
    dataset_dictionary = {}
    
    mock_entity = Entity(
        id="dataset_456",
        name="gdp-2021",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="GDP-2021"
    ):
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            dataset_relation=dataset_relation
        )
    
    # After removing year (-2021), we get "GDP", then title case
    assert "Gdp" in dataset_dictionary
    assert "dataset_456" in dataset_dictionary["Gdp"]
    
    mock_opengin_service.get_entities.assert_called_with(entity=Entity(id="dataset_456"))



@pytest.mark.asyncio
async def test_enrich_dataset_without_dataset_and_relation(data_service):
    """Test enrich_dataset raises BadRequestError when both dataset and relation are missing"""
    dataset_dictionary = {}

    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
        )
    
    assert "Dataset or dataset relation is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_enrich_dataset_with_internal_error(data_service, mock_opengin_service):
    """Test enrich_dataset handles internal errors"""
    dataset_relation = Relation(
        relatedEntityId="dataset_123",
        name="IS_ATTRIBUTE",
        direction="OUTGOING"
    )
    dataset_dictionary = {}
    
    # This will cause an error in get_entities
    mock_opengin_service.get_entities.side_effect = Exception("Network timeout")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            dataset_relation=dataset_relation
        )
    
    assert "An unexpected error occurred" in str(exc_info.value)
    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == "Network timeout"

# Tests for enrich_category
@pytest.mark.asyncio
async def test_enrich_category_with_category_entity(data_service):
    """Test enrich_category with a category entity"""
    category = Entity(
        id="category_123",
        name="encoded_category_name",
        kind=Kind(major="Category", minor="parentCategory")
    )
    categories_dictionary = {}
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="Decoded Category Name"
    ):
        await data_service.enrich_category(
            categories_dictionary=categories_dictionary,
            category=category
        )
    
    assert "Decoded Category Name" in categories_dictionary
    assert "category_123" in categories_dictionary["Decoded Category Name"]

@pytest.mark.asyncio
async def test_enrich_category_with_category_relation(data_service, mock_opengin_service):
    """Test enrich_category with a category relation"""
    category_relation = Relation(
        relatedEntityId="category_456",
        name="AS_CATEGORY",
        direction="OUTGOING"
    )
    categories_dictionary = {}
    
    mock_entity = Entity(
        id="category_456",
        name="encoded_category_name",
        kind=Kind(major="Category", minor="childCategory")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="Category from Relation"
    ):
        await data_service.enrich_category(
            categories_dictionary=categories_dictionary,
            category_relation=category_relation
        )
    
    assert "Category From Relation" in categories_dictionary
    assert "category_456" in categories_dictionary["Category From Relation"]
    
    mock_opengin_service.get_entities.assert_called_with(entity=Entity(id="category_456"))

@pytest.mark.asyncio
async def test_enrich_category_without_category_and_relation(data_service):
    """Test enrich_category raises BadRequestError when both category and relation are missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_category(categories_dictionary={})
    
    assert "No category or category relation provided" in str(exc_info.value)

@pytest.mark.asyncio
async def test_enrich_category_with_internal_error(data_service, mock_opengin_service):
    """Test enrich_category handles internal errors"""
    category_relation = Relation(
        relatedEntityId="category_456",
        name="AS_CATEGORY",
        direction="OUTGOING"
    )
    
    mock_opengin_service.get_entities.side_effect = Exception("Database Error")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.enrich_category(
            categories_dictionary={},
            category_relation=category_relation
        )
    
    assert "An unexpected error occurred" in str(exc_info.value)
    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == "Database Error"

# Tests for fetch_data_catalog
@pytest.mark.asyncio
async def test_fetch_data_catalog_without_parent_id(data_service, mock_opengin_service):
    """Test fetch_data_catalog fetches parent categories when no parent_id is provided"""
    mock_categories = [
        Entity(id="cat_1", name="encoded_1", kind=Kind(major="Category", minor="parentCategory")),
        Entity(id="cat_2", name="encoded_2", kind=Kind(major="Category", minor="parentCategory"))
    ]
    
    mock_opengin_service.get_entities.return_value = mock_categories
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["Category 1", "Category 2"]
    ):
        result = await data_service.fetch_data_catalog()
    
    assert "categories" in result
    assert "datasets" in result
    assert len(result["categories"]) == 2
    assert len(result["datasets"]) == 0
    
    names = [c["name"] for c in result["categories"]]
    assert "Category 1" in names
    assert "Category 2" in names
    
    mock_opengin_service.get_entities.assert_called_once()

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_entity_id_and_relations(data_service, mock_opengin_service):
    """Test fetch_data_catalog fetches child categories and datasets for a given entity_id"""
    entity_id = "parent_123"
    
    # Mock category and dataset relations
    category_relations = [
        Relation(relatedEntityId="cat_1", name="AS_CATEGORY", direction="OUTGOING"),
        Relation(relatedEntityId="cat_2", name="AS_CATEGORY", direction="OUTGOING")
    ]
    
    dataset_relations = [
        Relation(relatedEntityId="ds_1", name="IS_ATTRIBUTE", direction="OUTGOING")
    ]
    
    mock_opengin_service.fetch_relation.side_effect = [category_relations, dataset_relations]
    
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="cat_1", name="encoded_cat_1", kind=Kind(major="Category", minor="childCategory"))],
        [Entity(id="cat_2", name="encoded_cat_2", kind=Kind(major="Category", minor="childCategory"))],
        [Entity(id="ds_1", name="encoded_ds_1", kind=Kind(major="Dataset", minor="dataset"))]
    ]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["Child Category 1", "Child Category 2", "Dataset ds 2"]
    ):
        result = await data_service.fetch_data_catalog(category_ids=[entity_id])
    
    assert "categories" in result
    assert "datasets" in result
    assert len(result["categories"]) == 2
    assert len(result["datasets"]) == 1
    
    cat_names = [c["name"] for c in result["categories"]]
    assert "Child Category 1" in cat_names
    
    ds_names = [d["name"] for d in result["datasets"]]
    assert "Dataset Ds 2" in ds_names
    
    assert mock_opengin_service.fetch_relation.call_count == 2

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_entity_id_no_relations(data_service, mock_opengin_service):
    """Test fetch_data_catalog with entity_id but no relations"""
    entity_id = "parent_456"
    
    # Return empty lists for both category and dataset relations
    mock_opengin_service.fetch_relation.side_effect = [[], []]
    
    result = await data_service.fetch_data_catalog(category_ids=[entity_id])
    
    assert "categories" in result
    assert "datasets" in result
    assert not result["categories"]
    assert not result["datasets"]
    
    assert mock_opengin_service.fetch_relation.call_count == 2

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_only_categories(data_service, mock_opengin_service):
    """Test fetch_data_catalog with only category relations, no datasets"""
    entity_id = "parent_789"
    
    category_relations = [
        Relation(relatedEntityId="cat_1", name="AS_CATEGORY", direction="OUTGOING")
    ]
    
    mock_opengin_service.fetch_relation.side_effect = [category_relations, []]
    
    mock_opengin_service.get_entities.return_value = [
        Entity(id="cat_1", name="encoded_cat_1", kind=Kind(major="Category", minor="childCategory"))
    ]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="Solo Category"
    ):
        result = await data_service.fetch_data_catalog(category_ids=[entity_id])
    
    assert len(result["categories"]) == 1
    assert result["categories"][0]["name"] == "Solo Category"
    assert "cat_1" in result["categories"][0]["categoryIds"]
    assert not result["datasets"]

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_only_datasets(data_service, mock_opengin_service):
    """Test fetch_data_catalog with only dataset relations, no categories"""
    entity_id = "parent_101"
    
    dataset_relations = [
        Relation(relatedEntityId="ds_1", name="IS_ATTRIBUTE", direction="OUTGOING"),
        Relation(relatedEntityId="ds_2", name="IS_ATTRIBUTE", direction="OUTGOING")
    ]
    
    mock_opengin_service.fetch_relation.side_effect = [[], dataset_relations]
    
    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="ds_1", name="encoded_ds_1", kind=Kind(major="Dataset", minor="dataset"))],
        [Entity(id="ds_2", name="encoded_ds_2", kind=Kind(major="Dataset", minor="dataset"))]
    ]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["Dataset 1", "Dataset 2"]
    ):
        result = await data_service.fetch_data_catalog(category_ids=[entity_id])
    
    assert not result["categories"]
    assert len(result["datasets"]) == 2
    ds_names = [d["name"] for d in result["datasets"]]
    assert "Dataset 1" in ds_names
    assert "Dataset 2" in ds_names

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_internal_error(data_service, mock_opengin_service):
    """Test fetch_data_catalog handles internal errors"""
    mock_opengin_service.get_entities.side_effect = Exception("Database Error")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_data_catalog()
    
    assert "An unexpected error occurred" in str(exc_info.value)

# Tests for fetch_dataset_available_years
@pytest.mark.asyncio
async def test_fetch_dataset_available_years_success(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years with successful response"""
    
    dataset_ids = ["dataset_123", "dataset_124", "dataset_125"]
    
    # Mock entities that will be returned for each dataset_id
    mock_entity_1 = Entity(
        id="dataset_123",
        name="dataset_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2020-12-31T00:00:00Z",
        terminated=""
    )
    
    mock_entity_2 = Entity(
        id="dataset_124",
        name="dataset_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2021-06-15T00:00:00Z",
        terminated=""
    )
    
    mock_entity_3 = Entity(
        id="dataset_125",
        name="dataset_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2022-03-20T00:00:00Z",
        terminated=""
    )
    
    # get_entities is called 3 times (once per dataset_id) in asyncio.gather
    # Each call returns a list with one entity
    mock_opengin_service.get_entities.side_effect = [
        [mock_entity_1],
        [mock_entity_2],
        [mock_entity_3]
    ]
    
    # Mock the decode function - called once for dataset_name from first entity
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        return_value="Population Dataset"
    ):
        result = await data_service.fetch_dataset_available_years(dataset_ids)
    
    # Assertions
    assert "name" in result
    assert "years" in result  # Note: it's "years" not "year"
    assert result["name"] == "Population Dataset"
    assert len(result["years"]) == 3
    
    # Results should be sorted by year
    assert result["years"][0]["year"] == "2020"
    assert result["years"][0]["datasetId"] == "dataset_123"
    
    assert result["years"][1]["year"] == "2021"
    assert result["years"][1]["datasetId"] == "dataset_124"
    
    assert result["years"][2]["year"] == "2022"
    assert result["years"][2]["datasetId"] == "dataset_125"
    
    # Verify mocks were called correctly
    assert mock_opengin_service.get_entities.call_count == 3

@pytest.mark.asyncio
async def test_fetch_dataset_available_years_single_year(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years with single dataset"""
    
    dataset_ids = ["dataset_123"]
    
    mock_entity = Entity(
        id="dataset_123",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2023-06-15T00:00:00Z"
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        return_value="Single Year Dataset"
    ):
        result = await data_service.fetch_dataset_available_years(dataset_ids=dataset_ids)
    
    assert result["name"] == "Single Year Dataset"
    assert len(result["years"]) == 1
    assert result["years"][0]["year"] == "2023"

@pytest.mark.asyncio
async def test_fetch_dataset_available_years_without_dataset_ids(data_service):
    """Test fetch_dataset_available_years raises BadRequestError when dataset_ids is missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_dataset_available_years(dataset_ids=None)
    
    assert "Dataset ID list is required" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_empty_dataset_ids(data_service):
    """Test fetch_dataset_available_years raises BadRequestError when dataset_ids is empty"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_dataset_available_years(dataset_ids=[])
    
    assert "Dataset ID list is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_dataset_available_years_with_missing_created_date(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years when created date is missing"""
    dataset_ids = ["dataset_123"]
    
    mock_entity = Entity(
        id="dataset_123",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created=""
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        return_value="Dataset Without Date"
    ):
        result = await data_service.fetch_dataset_available_years(dataset_ids=dataset_ids)
    
    # When created date is None, the year should be "Unknown"
    assert result["name"] == "Dataset Without Date"
    assert len(result["years"]) == 1
    assert result["years"][0]["year"] == "Unknown"


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_with_internal_error(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years handles internal errors"""
    dataset_ids = ["dataset_123"]
    
    # Trigger error from get_entities call
    mock_opengin_service.get_entities.side_effect = Exception("Service unavailable")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_dataset_available_years(dataset_ids=dataset_ids)
    
    assert "An unexpected error occurred" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_multiple_years_sorted(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years with multiple years from same dataset"""
    dataset_ids = ["dataset_2019", "dataset_2024", "dataset_2022"]
    
    mock_entity_1 = Entity(
        id="dataset_2019",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2019-12-31T00:00:00Z"
    )
    
    mock_entity_2 = Entity(
        id="dataset_2024",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2024-03-15T00:00:00Z"
    )
    
    mock_entity_3 = Entity(
        id="dataset_2022",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular"),
        created="2022-07-20T00:00:00Z"
    )
    
    mock_opengin_service.get_entities.side_effect = [
        [mock_entity_1],
        [mock_entity_2],
        [mock_entity_3]
    ]
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        return_value="Multi-Year Dataset"
    ):
        result = await data_service.fetch_dataset_available_years(dataset_ids=dataset_ids)
    
    # After get_name_without_year removes -YYYY pattern, we get "Multi-Year Dataset", then title case
    assert result["name"] == "Multi Year Dataset"
    assert len(result["years"]) == 3
    # Check years are sorted correctly
    assert result["years"][0]["year"] == "2019"
    assert result["years"][0]["datasetId"] == "dataset_2019"
    assert result["years"][1]["year"] == "2022"
    assert result["years"][1]["datasetId"] == "dataset_2022"
    assert result["years"][2]["year"] == "2024"
    assert result["years"][2]["datasetId"] == "dataset_2024"

# Tests for lock functionality
@pytest.mark.asyncio
async def test_enrich_category_with_lock_prevents_race_condition(data_service):
    """Test that the instance-level lock prevents race conditions when multiple tasks update categories_dictionary concurrently"""
    
    categories_dictionary = {}
    
    # Create multiple categories with the same name
    categories = [
        Entity(id=f"cat_{i}", name=f"encoded_category_name_{i % 3}", kind=Kind(major="Category", minor="parentCategory"))
        for i in range(10)
    ]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=[f"Category {i % 3}" for i in range(10)]
    ):
        tasks = [
            data_service.enrich_category(
                categories_dictionary=categories_dictionary,
                category=category
            )
            for category in categories
        ]
        await asyncio.gather(*tasks)
    
    assert len(categories_dictionary) == 3  # Should have 3 unique category names (0, 1, 2)
    
    assert len(categories_dictionary["Category 0"]) == 4  # cat_0, cat_3, cat_6, cat_9
    assert len(categories_dictionary["Category 1"]) == 3  # cat_1, cat_4, cat_7
    assert len(categories_dictionary["Category 2"]) == 3  # cat_2, cat_5, cat_8
    
    all_ids = set()
    for ids in categories_dictionary.values():
        all_ids.update(ids)
    assert len(all_ids) == 10

@pytest.mark.asyncio
async def test_enrich_dataset_with_lock_prevents_race_condition(data_service, mock_opengin_service):
    """Test that the instance-level lock prevents race conditions when multiple tasks update dataset_dictionary concurrently"""
    
    dataset_dictionary = {}
    
    # Create multiple datasets with the same name (with years)
    datasets = [
        Entity(id=f"ds_{i}", name=f"dataset_name_{i % 2}-202{i % 2}", kind=Kind(major="Dataset", minor="tabular"))
        for i in range(8)
    ]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=[
            f"Employment-202{'0' if i % 2 == 0 else '1'}"
            for i in range(8)
        ]
    ):
        tasks = [
            data_service.enrich_dataset(
                dataset_dictionary=dataset_dictionary,
                dataset=ds
            )
            for ds in datasets
        ]
        await asyncio.gather(*tasks)
    
    # After removing years, all datasets should have the same name "Employment"
    assert len(dataset_dictionary) == 1
    assert "Employment" in dataset_dictionary
    assert len(dataset_dictionary["Employment"]) == 8  # All 8 dataset IDs
    
    all_ids = set()
    for ids in dataset_dictionary.values():
        all_ids.update(ids)
    assert len(all_ids) == 8
