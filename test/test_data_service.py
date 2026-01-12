import pytest
from src.exception.exceptions import InternalServerError, BadRequestError
from src.utils.util_functions import Util
from unittest.mock import AsyncMock, patch
from src.models.organisation_schemas import Entity, Relation, Kind, Label, Dataset, Category

# Tests for enrich_dataset
@pytest.mark.asyncio
async def test_enrich_dataset_with_dataset_entity(data_service, mock_opengin_service):
    """Test enrich_dataset with a dataset entity"""
    category_id = "category_123"
    dataset = Entity(id="dataset_123", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name": "Actual Dataset Name"
    }
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Actual Dataset Name"]
    ):
        result = await data_service.enrich_dataset(
            category_id=category_id,
            dataset=dataset
        )
    
    assert isinstance(result, Dataset)
    assert result.id == "dataset_123"
    assert result.label.id == "decoded_name"
    assert result.label.name == "Actual Dataset Name"
    assert result.parentId == category_id
    
    mock_opengin_service.get_metadata.assert_called_once_with(category_id)

@pytest.mark.asyncio
async def test_enrich_dataset_with_dataset_relation(data_service, mock_opengin_service):
    """Test enrich_dataset with a dataset relation"""
    category_id = "category_123"
    dataset_relation = Relation(
        relatedEntityId="dataset_456",
        name="IS_ATTRIBUTE",
        direction="OUTGOING"
    )
    
    mock_entity = Entity(
        id="dataset_456",
        name="encoded_name",
        kind=Kind(major="Dataset", minor="dataset")
    )
    
    mock_opengin_service.get_entity.return_value = [mock_entity]
    mock_opengin_service.get_metadata.return_value = {
        "decoded_name": "Dataset from Relation"
    }
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_name", "Dataset from Relation"]
    ):
        result = await data_service.enrich_dataset(
            category_id=category_id,
            dataset_relation=dataset_relation
        )
    
    assert isinstance(result, Dataset)
    assert result.id == "dataset_456"
    assert result.label.name == "Dataset from Relation"
    
    mock_opengin_service.get_entity.assert_called_once_with(entity=Entity(id="dataset_456"))

@pytest.mark.asyncio
async def test_enrich_dataset_without_metadata(data_service, mock_opengin_service):
    """Test enrich_dataset when metadata is not available"""
    category_id = "category_123"
    dataset = Entity(id="dataset_123", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    
    mock_opengin_service.get_metadata.return_value = None
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="decoded_name"
    ):
        result = await data_service.enrich_dataset(
            category_id=category_id,
            dataset=dataset
        )
    
    assert isinstance(result, Dataset)
    assert result.label.name == "Dataset Name is not provided"

@pytest.mark.asyncio
async def test_enrich_dataset_without_category_id(data_service):
    """Test enrich_dataset raises BadRequestError when category_id is missing"""
    dataset = Entity(id="dataset_123", name="encoded_name", kind=Kind(major="Dataset", minor="dataset"))
    
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_dataset(
            category_id=None,
            dataset=dataset
        )
    
    assert "Category ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_enrich_dataset_without_dataset_and_relation(data_service):
    """Test enrich_dataset raises BadRequestError when both dataset and relation are missing"""
    category_id = "category_123"
    
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_dataset(category_id=category_id)
    
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
    
    # This will cause an error in get_entity
    mock_opengin_service.get_entity.side_effect = Exception("Database connection failed")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.enrich_dataset(
            category_id=category_id,
            dataset_relation=dataset_relation
        )
    
    assert "An unexpected error occurred" in str(exc_info.value)

# Tests for enrich_category
@pytest.mark.asyncio
async def test_enrich_category_with_category_entity(data_service):
    """Test enrich_category with a category entity"""
    category = Entity(
        id="category_123",
        name="encoded_category_name",
        kind=Kind(major="Category", minor="parentCategory")
    )
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="Decoded Category Name"
    ):
        result = await data_service.enrich_category(category=category)
    
    assert isinstance(result, Category)
    assert result.id == "category_123"
    assert result.name == "Decoded Category Name"
    assert result.kind.major == "Category"

@pytest.mark.asyncio
async def test_enrich_category_with_category_relation(data_service, mock_opengin_service):
    """Test enrich_category with a category relation"""
    category_relation = Relation(
        relatedEntityId="category_456",
        name="AS_CATEGORY",
        direction="OUTGOING"
    )
    
    mock_entity = Entity(
        id="category_456",
        name="encoded_category_name",
        kind=Kind(major="Category", minor="childCategory")
    )
    
    mock_opengin_service.get_entity.return_value = [mock_entity]
    
    with patch(
        "src.services.data_service.Util.decode_protobuf_attribute_name",
        return_value="Category from Relation"
    ):
        result = await data_service.enrich_category(category_relation=category_relation)
    
    assert isinstance(result, Category)
    assert result.id == "category_456"
    assert result.name == "Category from Relation"
    
    mock_opengin_service.get_entity.assert_called_once_with(entity=Entity(id="category_456"))

@pytest.mark.asyncio
async def test_enrich_category_without_category_and_relation(data_service):
    """Test enrich_category raises BadRequestError when both category and relation are missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.enrich_category()
    
    assert "No category or category relation provided" in str(exc_info.value)

@pytest.mark.asyncio
async def test_enrich_category_with_internal_error(data_service, mock_opengin_service):
    """Test enrich_category handles internal errors"""
    category_relation = Relation(
        relatedEntityId="category_456",
        name="AS_CATEGORY",
        direction="OUTGOING"
    )
    
    mock_opengin_service.get_entity.side_effect = Exception("Service unavailable")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.enrich_category(category_relation=category_relation)
    
    assert "An unexpected error occurred" in str(exc_info.value)

# Tests for fetch_data_catalog
@pytest.mark.asyncio
async def test_fetch_data_catalog_without_parent_id(data_service, mock_opengin_service):
    """Test fetch_data_catalog fetches parent categories when no parent_id is provided"""
    mock_categories = [
        Entity(id="cat_1", name="encoded_1", kind=Kind(major="Category", minor="parentCategory")),
        Entity(id="cat_2", name="encoded_2", kind=Kind(major="Category", minor="parentCategory"))
    ]
    
    mock_opengin_service.get_entity.return_value = mock_categories
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        side_effect=["Category 1", "Category 2"]
    ):
        result = await data_service.fetch_data_catalog()
    
    assert "categories" in result
    assert "datasets" in result
    assert len(result["categories"]) == 2
    assert len(result["datasets"]) == 0
    assert result["categories"][0].id == "cat_1"
    assert result["categories"][1].id == "cat_2"
    
    mock_opengin_service.get_entity.assert_called_once()

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_parent_id_and_relations(data_service, mock_opengin_service):
    """Test fetch_data_catalog fetches child categories and datasets for a given parent_id"""
    parent_id = "parent_123"
    
    # Mock category and dataset relations
    category_relations = [
        Relation(relatedEntityId="cat_1", name="AS_CATEGORY", direction="OUTGOING"),
        Relation(relatedEntityId="cat_2", name="AS_CATEGORY", direction="OUTGOING")
    ]
    
    dataset_relations = [
        Relation(relatedEntityId="ds_1", name="IS_ATTRIBUTE", direction="OUTGOING")
    ]
    
    # Setup mock return for fetch_relation calls
    mock_opengin_service.fetch_relation.side_effect = [category_relations, dataset_relations]
    
    # Setup mock return for get_entity calls (for categories and datasets)
    mock_opengin_service.get_entity.side_effect = [
        [Entity(id="cat_1", name="encoded_cat_1", kind=Kind(major="Category", minor="childCategory"))],
        [Entity(id="cat_2", name="encoded_cat_2", kind=Kind(major="Category", minor="childCategory"))],
        [Entity(id="ds_1", name="encoded_ds_1", kind=Kind(major="Dataset", minor="dataset"))]
    ]
    
    # Setup mock return for get_metadata
    mock_opengin_service.get_metadata.return_value = {
        "decoded_ds_1": "Dataset 1"
    }
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        side_effect=["Child Category 1", "Child Category 2", "decoded_ds_1", "Dataset 1"]
    ):
        result = await data_service.fetch_data_catalog(parent_id=parent_id)
    
    assert "categories" in result
    assert "datasets" in result
    assert len(result["categories"]) == 2
    assert len(result["datasets"]) == 1
    assert result["categories"][0].id == "cat_1"
    assert result["datasets"][0].id == "ds_1"
    
    assert mock_opengin_service.fetch_relation.call_count == 2

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_parent_id_no_relations(data_service, mock_opengin_service):
    """Test fetch_data_catalog with parent_id but no relations"""
    parent_id = "parent_456"
    
    # Return empty lists for both category and dataset relations
    mock_opengin_service.fetch_relation.side_effect = [[], []]
    
    result = await data_service.fetch_data_catalog(parent_id=parent_id)
    
    assert "categories" in result
    assert "datasets" in result
    # When there are no tasks, asyncio.create_task(asyncio.sleep(0)) returns None or similar
    # The actual implementation checks for truthiness
    assert result["categories"] == [] or result["categories"] is None or not result["categories"]
    assert result["datasets"] == [] or result["datasets"] is None or not result["datasets"]
    
    assert mock_opengin_service.fetch_relation.call_count == 2

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_only_categories(data_service, mock_opengin_service):
    """Test fetch_data_catalog with only category relations, no datasets"""
    parent_id = "parent_789"
    
    category_relations = [
        Relation(relatedEntityId="cat_1", name="AS_CATEGORY", direction="OUTGOING")
    ]
    
    mock_opengin_service.fetch_relation.side_effect = [category_relations, []]
    
    mock_opengin_service.get_entity.return_value = [
        Entity(id="cat_1", name="encoded_cat_1", kind=Kind(major="Category", minor="childCategory"))
    ]
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        return_value="Solo Category"
    ):
        result = await data_service.fetch_data_catalog(parent_id=parent_id)
    
    assert len(result["categories"]) == 1
    assert result["datasets"] == [] or not result["datasets"]

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_only_datasets(data_service, mock_opengin_service):
    """Test fetch_data_catalog with only dataset relations, no categories"""
    parent_id = "parent_101"
    
    dataset_relations = [
        Relation(relatedEntityId="ds_1", name="IS_ATTRIBUTE", direction="OUTGOING"),
        Relation(relatedEntityId="ds_2", name="IS_ATTRIBUTE", direction="OUTGOING")
    ]
    
    mock_opengin_service.fetch_relation.side_effect = [[], dataset_relations]
    
    mock_opengin_service.get_entity.side_effect = [
        [Entity(id="ds_1", name="encoded_ds_1", kind=Kind(major="Dataset", minor="dataset"))],
        [Entity(id="ds_2", name="encoded_ds_2", kind=Kind(major="Dataset", minor="dataset"))]
    ]
    
    mock_opengin_service.get_metadata.return_value = {
        "decoded_ds_1": "Dataset 1",
        "decoded_ds_2": "Dataset 2"
    }
    
    with patch(
        "src.utils.util_functions.Util.decode_protobuf_attribute_name",
        side_effect=["decoded_ds_1", "Dataset 1", "decoded_ds_2", "Dataset 2"]
    ):
        result = await data_service.fetch_data_catalog(parent_id=parent_id)
    
    assert result["categories"] == [] or not result["categories"]
    assert len(result["datasets"]) == 2

@pytest.mark.asyncio
async def test_fetch_data_catalog_with_internal_error(data_service, mock_opengin_service):
    """Test fetch_data_catalog handles internal errors"""
    mock_opengin_service.get_entity.side_effect = Exception("Network error")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_data_catalog()
    
    assert "An unexpected error occurred" in str(exc_info.value)

