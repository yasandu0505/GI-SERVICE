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

# Tests for fetch_data_attributes
@pytest.mark.asyncio
async def test_fetch_data_attributes_success(data_service, mock_opengin_service):
    """Test fetch_data_attributes with successful response"""
    
    dataset_id = "dataset_123"
    
    # Mock dataset entity
    mock_entity = Entity(
        id=dataset_id,
        name="encoded_dataset_name",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    # Mock dataset relation
    mock_relation = Relation(
        relatedEntityId="category_456",
        name="IS_ATTRIBUTE",
        direction="INCOMING"
    )
    
    # Mock get_entities and fetch_relation to return values
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.fetch_relation.return_value = [mock_relation]
    
    # Mock get_attributes to return sample data
    mock_attributes = [
        {"attribute1": "value1", "attribute2": "value2"},
        {"attribute1": "value3", "attribute2": "value4"}
    ]
    mock_opengin_service.get_attributes.return_value = mock_attributes
    
    # Mock the formatted response from transform_data_for_chart
    mock_formatted_data = {
        "type": "tabular",
        "data": {
            "columns": ["attribute1", "attribute2"],
            "rows": [
                ["value1", "value2"],
                ["value3", "value4"]
            ]
        }
    }
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="decoded_dataset_name"), \
         patch("src.services.data_service.Util.transform_data_for_chart", return_value=mock_formatted_data):
        
        result = await data_service.fetch_data_attributes(dataset_id=dataset_id)
    
    # Assertions
    assert result is not None
    assert result["type"] == "tabular"
    assert "data" in result
    assert result["data"]["columns"] == ["attribute1", "attribute2"]
    assert len(result["data"]["rows"]) == 2
    
    # Verify mocks were called correctly
    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=dataset_id))
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=dataset_id,
        relation=Relation(name="IS_ATTRIBUTE", direction="INCOMING")
    )
    mock_opengin_service.get_attributes.assert_called_once_with(
        category_id="category_456",
        dataset_name="decoded_dataset_name"
    )

@pytest.mark.asyncio
async def test_fetch_data_attributes_without_dataset_id(data_service):
    """Test fetch_data_attributes raises BadRequestError when dataset_id is missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_data_attributes(dataset_id=None)
    
    assert "Dataset ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_data_attributes_empty_dataset_id(data_service):
    """Test fetch_data_attributes raises BadRequestError when dataset_id is empty string"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_data_attributes(dataset_id="")
    
    assert "Dataset ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_data_attributes_dataset_not_found(data_service, mock_opengin_service):
    """Test fetch_data_attributes when dataset entity is not found"""
    
    dataset_id = "nonexistent_dataset"
    
    # Mock get_entities to return empty list (dataset not found)
    mock_opengin_service.get_entities.return_value = []
    mock_opengin_service.fetch_relation.return_value = [
        Relation(relatedEntityId="category_123", name="IS_ATTRIBUTE")
    ]
    
    with pytest.raises(Exception):
        await data_service.fetch_data_attributes(dataset_id=dataset_id)

@pytest.mark.asyncio
async def test_fetch_data_attributes_no_relations_found(data_service, mock_opengin_service):
    """Test fetch_data_attributes when no relations are found for the dataset"""
    
    dataset_id = "dataset_456"
    
    mock_entity = Entity(
        id=dataset_id,
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    # Mock get_entities to return entity but fetch_relation returns empty list
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.fetch_relation.return_value = []
    
    with pytest.raises(Exception):
        await data_service.fetch_data_attributes(dataset_id=dataset_id)

@pytest.mark.asyncio
async def test_fetch_data_attributes_with_empty_attributes(data_service, mock_opengin_service):
    """Test fetch_data_attributes when get_attributes returns empty data"""
    
    dataset_id = "empty_dataset_555"
    
    mock_entity = Entity(
        id=dataset_id,
        name="encoded_empty_dataset",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    mock_relation = Relation(
        relatedEntityId="category_empty",
        name="IS_ATTRIBUTE"
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.fetch_relation.return_value = [mock_relation]
    mock_opengin_service.get_attributes.return_value = []
    
    mock_formatted_empty = {
        "type": "tabular",
        "data": {
            "columns": [],
            "rows": []
        }
    }
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="empty_dataset"), \
         patch("src.services.data_service.Util.transform_data_for_chart", return_value=mock_formatted_empty):
        
        result = await data_service.fetch_data_attributes(dataset_id=dataset_id)
    
    assert result["type"] == "tabular"
    assert result["data"]["columns"] == []
    assert result["data"]["rows"] == []

@pytest.mark.asyncio
async def test_fetch_data_attributes_with_get_entities_error(data_service, mock_opengin_service):
    """Test fetch_data_attributes handles errors from get_entities"""
    
    dataset_id = "dataset_error"
    
    mock_opengin_service.get_entities.side_effect = Exception("Service unavailable")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_data_attributes(dataset_id=dataset_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_data_attributes_with_fetch_relation_error(data_service, mock_opengin_service):
    """Test fetch_data_attributes handles errors from fetch_relation"""
    
    dataset_id = "dataset_rel_error"
    
    mock_entity = Entity(
        id=dataset_id,
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.fetch_relation.side_effect = Exception("Relation fetch failed")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_data_attributes(dataset_id=dataset_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_data_attributes_with_get_attributes_error(data_service, mock_opengin_service):
    """Test fetch_data_attributes handles errors from get_attributes"""
    
    dataset_id = "dataset_attr_error"
    
    mock_entity = Entity(
        id=dataset_id,
        name="encoded_name",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    mock_relation = Relation(
        relatedEntityId="category_123",
        name="IS_ATTRIBUTE"
    )
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.fetch_relation.return_value = [mock_relation]
    mock_opengin_service.get_attributes.side_effect = Exception("Attributes fetch failed")
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="dataset_name"):
        with pytest.raises(InternalServerError) as exc_info:
            await data_service.fetch_data_attributes(dataset_id=dataset_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_data_attributes_with_multiple_relations(data_service, mock_opengin_service):
    """Test fetch_data_attributes when multiple relations exist (uses first one)"""
    
    dataset_id = "multi_rel_dataset"
    
    mock_entity = Entity(
        id=dataset_id,
        name="encoded_multi",
        kind=Kind(major="Dataset", minor="tabular")
    )
    
    # Multiple relations, function should use the first one
    mock_relations = [
        Relation(relatedEntityId="category_first", name="IS_ATTRIBUTE"),
        Relation(relatedEntityId="category_second", name="IS_ATTRIBUTE")
    ]
    
    mock_opengin_service.get_entities.return_value = [mock_entity]
    mock_opengin_service.fetch_relation.return_value = mock_relations
    mock_opengin_service.get_attributes.return_value = []
    
    mock_formatted = {"type": "tabular", "data": {"columns": [], "rows": []}}
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="multi_dataset"), \
         patch("src.services.data_service.Util.transform_data_for_chart", return_value=mock_formatted):
        
        result = await data_service.fetch_data_attributes(dataset_id=dataset_id)
    
    # Verify get_attributes was called with the first relation's category_id
    mock_opengin_service.get_attributes.assert_called_once_with(
        category_id="category_first",
        dataset_name="multi_dataset"
    )

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

# Tests for find_root_department_or_minister
@pytest.mark.asyncio
async def test_find_root_department_or_minister_found_department(data_service, mock_opengin_service):
    """Test find_root_department_or_minister when the category itself is a department"""
    category_id = "category_dept_123"
    
    # Mock category entity that is a department
    mock_department = Entity(
        id=category_id,
        name="encoded_department_name",
        kind=Kind(major="Category", minor="department")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_department]
    
    result = await data_service.find_root_department_or_minister(category_id)
    
    assert result is not None
    assert result.id == category_id
    assert result.kind.minor == "department"
    mock_opengin_service.get_entities.assert_called_once_with(entity=Entity(id=category_id))

@pytest.mark.asyncio
async def test_find_root_department_or_minister_found_minister(data_service, mock_opengin_service):
    """Test find_root_department_or_minister when the category itself is a minister"""
    category_id = "category_minister_456"
    
    # Mock category entity that is a minister
    mock_minister = Entity(
        id=category_id,
        name="encoded_minister_name",
        kind=Kind(major="Category", minor="minister")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_minister]
    
    result = await data_service.find_root_department_or_minister(category_id)
    
    assert result is not None
    assert result.id == category_id
    assert result.kind.minor == "minister"

@pytest.mark.asyncio
async def test_find_root_department_or_minister_recursive_traversal(data_service, mock_opengin_service):
    """Test find_root_department_or_minister correctly traverses up the hierarchy"""
    child_category_id = "child_category_789"
    parent_category_id = "parent_category_012"
    grandparent_category_id = "grandparent_dept_345"
    
    # Mock child category (not a department/minister)
    mock_child_category = Entity(
        id=child_category_id,
        name="encoded_child",
        kind=Kind(major="Category", minor="subCategory")
    )
    
    # Mock parent category (not a department/minister)
    mock_parent_category = Entity(
        id=parent_category_id,
        name="encoded_parent",
        kind=Kind(major="Category", minor="parentCategory")
    )
    
    # Mock grandparent category (is a department)
    mock_grandparent_department = Entity(
        id=grandparent_category_id,
        name="encoded_grandparent_dept",
        kind=Kind(major="Category", minor="department")
    )
    
    # Mock relation from child to parent
    mock_child_to_parent_relation = Relation(
        relatedEntityId=parent_category_id,
        name="AS_CATEGORY",
        direction="INCOMING"
    )
    
    # Mock relation from parent to grandparent
    mock_parent_to_grandparent_relation = Relation(
        relatedEntityId=grandparent_category_id,
        name="AS_CATEGORY",
        direction="INCOMING"
    )
    
    # Setup mock behavior:
    # 1st call: get_entities for child_category
    # 2nd call: fetch_relation for child_category relations
    # 3rd call: get_entities for parent_category
    # 4th call: fetch_relation for parent_category relations
    # 5th call: get_entities for grandparent_category (department)
    mock_opengin_service.get_entities.side_effect = [
        [mock_child_category],
        [mock_parent_category],
        [mock_grandparent_department]
    ]
    
    mock_opengin_service.fetch_relation.side_effect = [
        [mock_child_to_parent_relation],
        [mock_parent_to_grandparent_relation]
    ]
    
    result = await data_service.find_root_department_or_minister(child_category_id)
    
    assert result is not None
    assert result.id == grandparent_category_id
    assert result.kind.minor == "department"
    
    # Verify recursive calls
    assert mock_opengin_service.get_entities.call_count == 3
    assert mock_opengin_service.fetch_relation.call_count == 2

@pytest.mark.asyncio
async def test_find_root_department_or_minister_no_parent_found(data_service, mock_opengin_service):
    """Test find_root_department_or_minister when no parent category is found"""
    category_id = "orphan_category_111"
    
    # Mock category that is not a department/minister
    mock_category = Entity(
        id=category_id,
        name="encoded_orphan",
        kind=Kind(major="Category", minor="childCategory")
    )
    
    mock_opengin_service.get_entities.return_value = [mock_category]
    # Return empty list (no parent relations found)
    mock_opengin_service.fetch_relation.return_value = []
    
    result = await data_service.find_root_department_or_minister(category_id)
    
    assert result is None

@pytest.mark.asyncio
async def test_find_root_department_or_minister_without_category_id(data_service):
    """Test find_root_department_or_minister raises BadRequestError when category_id is missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.find_root_department_or_minister(category_id=None)
    
    assert "Category ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_find_root_department_or_minister_with_empty_category_id(data_service):
    """Test find_root_department_or_minister raises BadRequestError when category_id is empty"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.find_root_department_or_minister(category_id="")
    
    assert "Category ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_find_root_department_or_minister_with_internal_error(data_service, mock_opengin_service):
    """Test find_root_department_or_minister handles internal errors"""
    category_id = "error_category_222"
    
    mock_opengin_service.get_entities.side_effect = Exception("Database connection failed")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.find_root_department_or_minister(category_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

# Tests for fetch_dataset_root
@pytest.mark.asyncio
async def test_fetch_dataset_root_success_with_department(data_service, mock_opengin_service):
    """Test fetch_dataset_root successfully finds a department"""
    dataset_id = "dataset_root_123"
    category_id = "category_456"
    department_id = "department_789"
    
    # Mock relation from dataset to category
    mock_dataset_relation = Relation(
        relatedEntityId=category_id,
        name="IS_ATTRIBUTE",
        direction="INCOMING"
    )
    
    # Mock department entity
    mock_department = Entity(
        id=department_id,
        name="encoded_department_name",
        kind=Kind(major="Category", minor="department")
    )
    
    mock_opengin_service.fetch_relation.return_value = [mock_dataset_relation]
    mock_opengin_service.get_entities.return_value = [mock_department]
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="Ministry of Health"):
        result = await data_service.fetch_dataset_root(dataset_id)
    
    assert result is not None
    assert result["id"] == department_id
    assert result["name"] == "Ministry of Health"
    assert result["type"] == "department"
    
    # Verify fetch_relation was called with correct parameters
    mock_opengin_service.fetch_relation.assert_called_once_with(
        entityId=dataset_id,
        relation=Relation(name="IS_ATTRIBUTE", direction="INCOMING")
    )

@pytest.mark.asyncio
async def test_fetch_dataset_root_success_with_minister(data_service, mock_opengin_service):
    """Test fetch_dataset_root successfully finds a minister"""
    dataset_id = "dataset_minister_321"
    category_id = "category_654"
    minister_id = "minister_987"
    
    # Mock relation from dataset to category
    mock_dataset_relation = Relation(
        relatedEntityId=category_id,
        name="IS_ATTRIBUTE",
        direction="INCOMING"
    )
    
    # Mock category (not a department/minister)
    mock_category = Entity(
        id=category_id,
        name="encoded_category",
        kind=Kind(major="Category", minor="subCategory")
    )
    
    # Mock minister entity
    mock_minister = Entity(
        id=minister_id,
        name="encoded_minister_name",
        kind=Kind(major="Category", minor="minister")
    )
    
    # Mock relation from category to minister
    mock_category_relation = Relation(
        relatedEntityId=minister_id,
        name="AS_CATEGORY",
        direction="INCOMING"
    )
    
    # Setup call sequence:
    # 1st call: fetch_relation for dataset -> category
    # 2nd call: get_entities for category
    # 3rd call: fetch_relation for category -> parent
    # 4th call: get_entities for minister
    mock_opengin_service.fetch_relation.side_effect = [
        [mock_dataset_relation],
        [mock_category_relation]
    ]
    
    mock_opengin_service.get_entities.side_effect = [
        [mock_category],
        [mock_minister]
    ]
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="Prime Minister"):
        result = await data_service.fetch_dataset_root(dataset_id)
    
    assert result is not None
    assert result["id"] == minister_id
    assert result["name"] == "Prime Minister"
    assert result["type"] == "minister"

@pytest.mark.asyncio
async def test_fetch_dataset_root_without_dataset_id(data_service):
    """Test fetch_dataset_root raises BadRequestError when dataset_id is missing"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_dataset_root(dataset_id=None)
    
    assert "Dataset ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_dataset_root_with_empty_dataset_id(data_service):
    """Test fetch_dataset_root raises BadRequestError when dataset_id is empty"""
    with pytest.raises(BadRequestError) as exc_info:
        await data_service.fetch_dataset_root(dataset_id="")
    
    assert "Dataset ID is required" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_dataset_root_no_relation_found(data_service, mock_opengin_service):
    """Test fetch_dataset_root when no relation is found for the dataset"""
    dataset_id = "orphan_dataset_555"
    
    # Return empty list (no relations found)
    mock_opengin_service.fetch_relation.return_value = []
    
    result = await data_service.fetch_dataset_root(dataset_id)
    
    assert result is not None
    assert "detail" in result
    assert result["detail"] == "No relation found for dataset"

@pytest.mark.asyncio
async def test_fetch_dataset_root_no_root_entity_found(data_service, mock_opengin_service):
    """Test fetch_dataset_root when no root department or minister is found"""
    dataset_id = "rootless_dataset_666"
    category_id = "rootless_category_777"
    
    # Mock relation from dataset to category
    mock_dataset_relation = Relation(
        relatedEntityId=category_id,
        name="IS_ATTRIBUTE",
        direction="INCOMING"
    )
    
    # Mock category (not a department/minister)
    mock_category = Entity(
        id=category_id,
        name="encoded_category",
        kind=Kind(major="Category", minor="subCategory")
    )
    
    mock_opengin_service.fetch_relation.side_effect = [
        [mock_dataset_relation],
        []  # No parent relations found
    ]
    
    mock_opengin_service.get_entities.return_value = [mock_category]
    
    result = await data_service.fetch_dataset_root(dataset_id)
    
    assert result is not None
    assert "detail" in result
    assert result["detail"] == "Dataset not found"

@pytest.mark.asyncio
async def test_fetch_dataset_root_with_fetch_relation_error(data_service, mock_opengin_service):
    """Test fetch_dataset_root handles errors from fetch_relation"""
    dataset_id = "error_dataset_888"
    
    mock_opengin_service.fetch_relation.side_effect = Exception("Relation service unavailable")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_dataset_root(dataset_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_dataset_root_with_find_root_error(data_service, mock_opengin_service):
    """Test fetch_dataset_root handles errors from find_root_department_or_minister"""
    dataset_id = "error_dataset_999"
    category_id = "error_category_000"
    
    # Mock relation from dataset to category
    mock_dataset_relation = Relation(
        relatedEntityId=category_id,
        name="IS_ATTRIBUTE",
        direction="INCOMING"
    )
    
    mock_opengin_service.fetch_relation.return_value = [mock_dataset_relation]
    # get_entities will throw error during find_root_department_or_minister
    mock_opengin_service.get_entities.side_effect = Exception("Entity service error")
    
    with pytest.raises(InternalServerError) as exc_info:
        await data_service.fetch_dataset_root(dataset_id)
    
    assert "An unexpected error occurred" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_dataset_root_multiple_relations_uses_first(data_service, mock_opengin_service):
    """Test fetch_dataset_root uses the first relation when multiple relations exist"""
    dataset_id = "multi_relation_dataset"
    category_id_1 = "category_first"
    category_id_2 = "category_second"
    department_id = "department_main"
    
    # Mock multiple relations (should use first one)
    mock_relations = [
        Relation(relatedEntityId=category_id_1, name="IS_ATTRIBUTE", direction="INCOMING"),
        Relation(relatedEntityId=category_id_2, name="IS_ATTRIBUTE", direction="INCOMING")
    ]
    
    # Mock department entity
    mock_department = Entity(
        id=department_id,
        name="encoded_dept",
        kind=Kind(major="Category", minor="department")
    )
    
    mock_opengin_service.fetch_relation.return_value = mock_relations
    mock_opengin_service.get_entities.return_value = [mock_department]
    
    with patch("src.services.data_service.Util.decode_protobuf_attribute_name", return_value="Main Department"):
        result = await data_service.fetch_dataset_root(dataset_id)
    
    assert result is not None
    assert result["id"] == department_id
    # Verify get_entities was called with the first relation's category_id
    mock_opengin_service.get_entities.assert_called_with(entity=Entity(id=category_id_1))
