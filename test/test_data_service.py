import pytest
import asyncio
from src.exception.exceptions import InternalServerError, BadRequestError
from unittest.mock import patch
from src.models.organisation_schemas import Entity, Relation, Kind, Dataset, Category

# Tests for enrich_dataset
@pytest.mark.asyncio
async def test_enrich_dataset_with_dataset_entity(data_service, mock_opengin_service):
    """Test enrich_dataset with a dataset entity"""
    category_id = "category_123"
    dataset = Entity(id="dataset_123", name="encoded_name", kind=Kind(major="Dataset", minor="tabular"))
    dataset_dictionary = {}
    metadata_cache = {}
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Actual Dataset Name"]
    ):
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            category_id=category_id,
            metadata_cache=metadata_cache,
            dataset=dataset
        )
    
    assert "Actual Dataset Name" in dataset_dictionary

@pytest.mark.asyncio
async def test_enrich_dataset_with_dataset_relation(data_service, mock_opengin_service):
    """Test enrich_dataset with a dataset relation"""
    category_id = "category_123"
    dataset_relation = Relation(
        relatedEntityId="dataset_456",
        name="IS_ATTRIBUTE",
        direction="OUTGOING"
    )
    dataset_dictionary = {}
    metadata_cache = {}
    
    mock_entity = Entity(
        id="dataset_456",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name": "Dataset from Relation"
    }
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Dataset from Relation"]
    ):
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            category_id=category_id,
            metadata_cache=metadata_cache,
            dataset_relation=dataset_relation
        )
    
    assert "Dataset From Relation" in dataset_dictionary
    assert "dataset_456" in dataset_dictionary["Dataset From Relation"]
    
    mock_opengin_service.get_entities.assert_called_with(entity=Entity(id="dataset_456"))

@pytest.mark.asyncio
async def test_enrich_dataset_without_category_id(data_service):
    """Test enrich_dataset raises BadRequestError when category_id is missing"""
    dataset = Entity(id="dataset_123", name="encoded_name", kind=Kind(major="Dataset", minor="tabular"))
    dataset_dictionary = {}
    metadata_cache = {}

    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            category_id=None,
            metadata_cache=metadata_cache,
            dataset=dataset
        )
    
    assert "Category ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_enrich_dataset_without_dataset_and_relation(data_service):
    """Test enrich_dataset raises BadRequestError when both dataset and relation are missing"""
    category_id = "category_123"
    dataset_dictionary = {}
    metadata_cache = {}

    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            category_id=category_id,
            metadata_cache=metadata_cache
        )
    
    assert "Dataset or dataset relation is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_enrich_dataset_with_internal_error(data_service, mock_opengin_service):
    """Test enrich_dataset handles internal errors"""
    category_id = "category_123"
    dataset_relation = Relation(
        relatedEntityId="dataset_123",
        name="IS_ATTRIBUTE",
        direction="OUTGOING"
    )
    dataset_dictionary = {}
    metadata_cache = {}
    
    # This will cause an error in get_entities
    mock_opengin_service.get_entities.side_effect = Exception("Network timeout")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.enrich_dataset(
            dataset_dictionary=dataset_dictionary,
            category_id=category_id,
            metadata_cache=metadata_cache,
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
    
    # Setup mock return for get_metadata
    mock_opengin_service.get_metadata.return_value = {
        "decoded_ds_1": "Dataset_ds_2"
    }
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["Child Category 1", "Child Category 2", "decoded_ds_1", "Dataset_ds_2"]
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
    
    mock_opengin_service.get_metadata.return_value = {
        "decoded_ds_1": "Dataset 1",
        "decoded_ds_2": "Dataset 2"
    }
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_ds_1", "Dataset 1", "decoded_ds_2", "Dataset 2"]
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

# Tests for enrich_dataset_years
@pytest.mark.asyncio
async def test_enrich_dataset_years_success(data_service):
    """Test enrich_dataset_years extracts year from dataset relation"""
    dataset_relation = Relation(
        relatedEntityId="dataset_123",
        name="IS_ATTRIBUTE",
        direction="OUTGOING",
        startTime="2023-10-15T00:00:00Z"
    )
    
    result = await data_service.enrich_dataset_years(dataset_relation=dataset_relation)
    
    assert result["datasetId"] == "dataset_123"
    assert result["year"] == "2023"


@pytest.mark.asyncio
async def test_enrich_dataset_years_different_year(data_service):
    """Test enrich_dataset_years with different year"""
    dataset_relation = Relation(
        relatedEntityId="dataset_456",
        name="IS_ATTRIBUTE",
        direction="OUTGOING",
        startTime="2020-01-01T00:00:00Z"
    )
    
    result = await data_service.enrich_dataset_years(dataset_relation=dataset_relation)
    
    assert result["datasetId"] == "dataset_456"
    assert result["year"] == "2020"


@pytest.mark.asyncio
async def test_enrich_dataset_years_without_relation(data_service):
    """Test enrich_dataset_years raises BadRequestError when dataset_relation is missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_dataset_years(dataset_relation=None)
    
    assert "Dataset relation is required" in str(exc_info.value)

# Tests for fetch_dataset_available_years
@pytest.mark.asyncio
async def test_fetch_dataset_available_years_success(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years with successful response"""
    from src.exception.exceptions import NotFoundError
    
    category_id = "category_123"
    
    dataset_relations = [
        Relation(
            relatedEntityId="ds_1",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2020-01-01T00:00:00Z"
        ),
        Relation(
            relatedEntityId="ds_2",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2021-01-01T00:00:00Z"
        ),
        Relation(
            relatedEntityId="ds_3",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2022-01-01T00:00:00Z"
        )
    ]
    
    mock_opengin_service.fetch_relation.return_value = dataset_relations
    
    # Mock for enrich_dataset call
    mock_opengin_service.get_entity.return_value = [
        Entity(id="ds_1", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    ]
    
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name": "Population Dataset"
    }
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Population Dataset"]
    ):
        result = await data_service.fetch_dataset_available_years(category_id=category_id)
    
    assert "name" in result
    assert "year" in result
    assert result["name"] == "Population Dataset"
    assert len(result["year"]) == 3
    assert result["year"][0]["year"] == "2020"
    assert result["year"][1]["year"] == "2021"
    assert result["year"][2]["year"] == "2022"
    assert result["year"][0]["datasetId"] == "ds_1"
    
    mock_opengin_service.fetch_relation.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_single_year(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years with single dataset"""
    from src.exception.exceptions import NotFoundError
    
    category_id = "category_456"
    
    dataset_relations = [
        Relation(
            relatedEntityId="ds_1",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2023-06-15T00:00:00Z"
        )
    ]
    
    mock_opengin_service.fetch_relation.return_value = dataset_relations
    mock_opengin_service.get_entity.return_value = [
        Entity(id="ds_1", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    ]
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name": "Single Year Dataset"
    }
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Single Year Dataset"]
    ):
        result = await data_service.fetch_dataset_available_years(category_id=category_id)
    
    assert result["name"] == "Single Year Dataset"
    assert len(result["year"]) == 1
    assert result["year"][0]["year"] == "2023"


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_without_category_id(data_service):
    """Test fetch_dataset_available_years raises BadRequestError when category_id is missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_dataset_available_years(category_id=None)
    
    assert "Category ID is required" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_empty_category_id(data_service):
    """Test fetch_dataset_available_years raises BadRequestError when category_id is empty"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_dataset_available_years(category_id="")
    
    assert "Category ID is required" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_no_datasets(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years raises NotFoundError when no datasets found"""
    from src.exception.exceptions import NotFoundError
    
    category_id = "category_empty"
    
    mock_opengin_service.fetch_relation.return_value = []
    
    with pytest.raises(NotFoundError) as exc_info:
        await data_service.fetch_dataset_available_years(category_id=category_id)
    
    assert "No datasets found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_with_metadata_unavailable(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years when metadata is not available"""
    category_id = "category_789"
    
    dataset_relations = [
        Relation(
            relatedEntityId="ds_1",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2020-01-01T00:00:00Z"
        )
    ]
    
    mock_opengin_service.fetch_relation.return_value = dataset_relations
    mock_opengin_service.get_entity.return_value = [
        Entity(id="ds_1", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    ]
    mock_opengin_service.get_metadata.return_value = None
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        return_value="decoded_name"
    ):
        result = await data_service.fetch_dataset_available_years(category_id=category_id)
    
    assert result["name"] == "Dataset Name is not provided"
    assert len(result["year"]) == 1
    assert result["year"][0]["year"] == "2020"


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_with_internal_error(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years handles internal errors"""
    category_id = "category_error"
    
    mock_opengin_service.fetch_relation.side_effect = Exception("Service unavailable")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_dataset_available_years(category_id=category_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_available_years_multiple_years_sorted(data_service, mock_opengin_service):
    """Test fetch_dataset_available_years with multiple years from same dataset"""
    category_id = "category_multi"
    
    dataset_relations = [
        Relation(
            relatedEntityId="ds_2019",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2019-12-31T00:00:00Z"
        ),
        Relation(
            relatedEntityId="ds_2024",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2024-03-15T00:00:00Z"
        ),
        Relation(
            relatedEntityId="ds_2022",
            name="IS_ATTRIBUTE",
            direction="OUTGOING",
            startTime="2022-07-20T00:00:00Z"
        )
    ]
    
    mock_opengin_service.fetch_relation.return_value = dataset_relations
    mock_opengin_service.get_entity.return_value = [
        Entity(id="ds_2019", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    ]
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name": "Multi-Year Dataset"
    }
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Multi-Year Dataset"]
    ):
        result = await data_service.fetch_dataset_available_years(category_id=category_id)
    
    assert result["name"] == "Multi-Year Dataset"
    assert len(result["year"]) == 3
    # Check years are extracted correctly (order depends on relation order, not sorted by year)
    years = [item["year"] for item in result["year"]]
    assert "2019" in years
    assert "2024" in years
    assert "2022" in years
    root_cause = exc_info.value.__cause__
    assert isinstance(root_cause, Exception)
    assert str(root_cause) == "Database Error"

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
    category_id = "category_123"
    
    # Create multiple datasets with the same name
    datasets = [
        Entity(id=f"ds_{i}", name=f"encoded_name_{i % 2}", kind=Kind(major="Dataset", minor="tabular"))
        for i in range(8)
    ]
    
    metadata_cache = {}
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name_0": "Dataset A",
        "decoded_name_1": "Dataset B"
    }
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=[
            f"decoded_name_{i % 2}" if j == 0 else f"Dataset {'A' if i % 2 == 0 else 'B'}"
            for i in range(8)
            for j in range(2)
        ]
    ):

        tasks = [
            data_service.enrich_dataset(
                dataset_dictionary=dataset_dictionary,
                category_id=category_id,
                metadata_cache=metadata_cache,
                dataset=ds
            )
            for ds in datasets
        ]
        await asyncio.gather(*tasks)
    
    assert len(dataset_dictionary) == 2  # Should have 2 unique dataset names (A and B)
    
    assert len(dataset_dictionary["Dataset A"]) == 4  # ds_0, ds_2, ds_4, ds_6
    assert len(dataset_dictionary["Dataset B"]) == 4  # ds_1, ds_3, ds_5, ds_7
    
    all_ids = set()
    for ids in dataset_dictionary.values():
        all_ids.update(ids)
    assert len(all_ids) == 8
