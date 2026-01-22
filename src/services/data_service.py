import logging  
import asyncio
from typing import Dict
from src.exception.exceptions import InternalServerError, NotFoundError
from src.exception.exceptions import BadRequestError
from src.models.organisation_schemas import Label
from src.models.organisation_schemas import Dataset
from src.models.organisation_schemas import Relation
from src.models.organisation_schemas import Category
from src.utils.util_functions import Util
from src.models.organisation_schemas import Kind
from src.models.organisation_schemas import Entity
from aiohttp import ClientSession
from src.utils import http_client

logger = logging.getLogger(__name__)

class DataService:
    """
    This service is responsible for executing aggregate functions by calling the OpenGINService and processing the data related tasks including fetching data catalog and datasets.
    """

    def __init__(self, config: dict, opengin_service):
        self.config = config   
        self.opengin_service = opengin_service 
        self.lock = asyncio.Lock()

    @property
    def session(self) -> ClientSession:
        """Access the global session"""
        return http_client.session

    async def enrich_dataset(self, dataset_dictionary: dict[str, list[str]], category_id: str, metadata_cache: dict, dataset: Entity = None, dataset_relation: Relation = None):
        """
        Enriches the dataset with the decoded name.
        
        Args:
            category_id (str): The ID of the parent category to the dataset.
            dataset (Entity, optional): The dataset to enrich. Defaults to None.
            dataset_relation (Relation, optional): The dataset relation to enrich. Defaults to None.
            dataset_dictionary (dict[str, list[str]], optional): The dictionary to store the enriched dataset. Defaults to None.
            metadata_cache (dict[str, dict], optional): The cache for metadata. Defaults to None.
        Returns:
            Dataset: The enriched dataset.
        """
        try:
            if not category_id:
                raise BadRequestError("Category ID is required")

            if not dataset and not dataset_relation:
                raise BadRequestError("Dataset or dataset relation is required")

            if dataset_relation:
                dataset_id = dataset_relation.relatedEntityId
                datasets = await self.opengin_service.get_entities(entity=Entity(id=dataset_id))
                dataset = datasets[0]

            decoded_name = Util.decode_protobuf_attribute_name(dataset.name)

            actual_name = Util.decode_protobuf_attribute_name(metadata_cache.get(decoded_name))
            
            actual_name_title_case = Util.to_title_case(actual_name)

            # Append the dataset id to the dataset dictionary with lock protection
            async with self.lock:
                dataset_dictionary.setdefault(actual_name_title_case, set()).add(dataset.id)

        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to enrich dataset {e}")
            raise InternalServerError("An unexpected error occurred") from e


    async def enrich_category(self, categories_dictionary: dict[str, list[str]], category: Entity = None, category_relation: Relation = None):
        """This function takes the categories_dictionary variable and appends the category id to the categories dictionary. 
        This is done to avoid duplicate category names and improve the performance using instance data access by key
        
        Args:
            categories_dictionary (dict[str, list[str]]): The dictionary to append the category id to.
            category (Entity, optional): The category to enrich. Defaults to None.
            category_relation (Relation, optional): The category relation to enrich. Defaults to None.
        
        Returns:
            None
        """

        try:
            if not category and not category_relation:
                raise BadRequestError("No category or category relation provided")
            
            if category_relation:
                category_id = category_relation.relatedEntityId
                categories = await self.opengin_service.get_entities(entity=Entity(id=category_id))
                category = categories[0]

            decoded_name = Util.decode_protobuf_attribute_name(category.name)
            # Append the category id to the category dictionary
            actual_name_title_case = Util.to_title_case(decoded_name)

            # Use lock if provided for thread-safe dictionary updates
            async with self.lock:
                categories_dictionary.setdefault(actual_name_title_case, set()).add(category.id)
        
        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to enrich category {e}")
            raise InternalServerError("An unexpected error occurred") from e

    # helper: convert dictionary to a list
    @staticmethod
    def convert_dict_to_list(dictionary: dict, key_name: str, value_name: str) -> list:
        """
        Convert a dictionary to a list of dictionaries.
        Example: {"category1": ["cat_1","cat_2"]} -> [{"name": "category1", "categoryIds": ["cat_1","cat_2"]}]

        Args:
            dictionary (dict): The dictionary to convert.
            key_name (str): The name of the key to use for the category.
            value_name (str): The name of the key to use for the values.

        Returns:
            list: A list of dictionaries.
        """
        list_of_dicts = [
            {
                key_name: category,
                value_name: ids
            }
            for category, ids in dictionary.items()
        ]

        return list_of_dicts

    async def fetch_data_catalog(self, category_ids: list[str] = None):
        """
        Fetches the data catalog for a list of entity IDs. If no entity IDs are provided, it fetches all parent categories. Otherwise it fetches the child categories and datasets for each entity ID.
        This category and dataset dictionary will store the decoded category name as key and list of category ids as value, such that entities with the same name will have this name as the key and all their ids as the value.
        Example: {"category1": ["cat_1", "cat_2"], "category2": ["cat_3", "cat_4"]}
        
        Args:
            category_ids (list[str], optional): The list of category IDs. Defaults to None.
        
        Returns:
            dict: A dictionary containing the list of categories and datasets.
        
        Note: This function is used to fetch the data catalog per user click. 
        """

        categories_dictionary : Dict[str, list[str]] = {}
        dataset_dictionary: Dict[str, list[str]] = {}
        
        try:
            if not category_ids:
                entity = Entity(kind=Kind(major="Category", minor="parentCategory"))
                parentCategories = await self.opengin_service.get_entities(entity=entity)

                enrich_category_task = [self.enrich_category(categories_dictionary=categories_dictionary, category=category) for category in parentCategories]
                await asyncio.gather(*enrich_category_task)

                categories = self.convert_dict_to_list(categories_dictionary,"name","categoryIds")

                return {
                    "categories": categories,
                    "datasets": []
                    }
            
            else:
                category_relation_instance = Relation(name="AS_CATEGORY", direction="OUTGOING")
                dataset_relation_instance = Relation(name="IS_ATTRIBUTE", direction="OUTGOING")
                
                fetch_category_relation_tasks = [self.opengin_service.fetch_relation(entityId=category_id, relation=category_relation_instance) for category_id in category_ids]
                fetch_dataset_relation_tasks = [self.opengin_service.fetch_relation(entityId=category_id, relation=dataset_relation_instance) for category_id in category_ids]
                category_relations, dataset_relations = await asyncio.gather(
                    asyncio.gather(*fetch_category_relation_tasks), 
                    asyncio.gather(*fetch_dataset_relation_tasks),
                )

                if dataset_relations:
                    # cache the metadata to reduce the latency
                    fetch_metadata_tasks = [
                        self.opengin_service.get_metadata(entityId=category_id)for category_id in category_ids]
                    metadata_results = await asyncio.gather(*fetch_metadata_tasks)
                    metadata_cache = dict(zip(category_ids, metadata_results))

                # tasks for parallel execution
                category_enrich_tasks = [
                    self.enrich_category(category_relation=relation, categories_dictionary=categories_dictionary) 
                    for sublist in category_relations 
                    for relation in sublist
                    ]
                dataset_enrich_tasks = [
                    self.enrich_dataset(dataset_relation=relation, category_id=category_id, metadata_cache=metadata_cache.get(category_id, {}), dataset_dictionary=dataset_dictionary)
                    for sublist, category_id in zip(dataset_relations, category_ids)
                    for relation in sublist
                    ]

                # parallel execution of tasks
                await asyncio.gather(
                    asyncio.gather(*category_enrich_tasks),
                    asyncio.gather(*dataset_enrich_tasks)
                )

                # Convert the categories_dictionary and dataset_dictionary to the format required
                categories = self.convert_dict_to_list(categories_dictionary, "name", "categoryIds")
                datasets = self.convert_dict_to_list(dataset_dictionary, "name", "datasetIds")

                return {
                    "categories": categories,
                    "datasets": datasets
                }
                
        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to fetch data catalog {e}")
            raise InternalServerError("An unexpected error occurred") from e
    
    async def fetch_dataset_available_years(self, dataset_ids: list[str]):
        """
        Fetches the available years for a group of related dataset IDs.

        This function assumes all provided dataset IDs represent different yearly
        instances of the same logical dataset. It uses the first dataset ID in the
        list to find the parent category and determine the common dataset name.
        
        Args:
            dataset_ids (list[str]): List of dataset IDs that belong to the same group.
            
        Returns:
            dict: A dictionary containing the common dataset name and a list of
                  available years with their corresponding dataset IDs.
        """
        try:
            if not dataset_ids:
                raise BadRequestError("Dataset ID list is required")

            # prepare the dataset entities
            dataset_entities = [Entity(id=dataset_id) for dataset_id in dataset_ids]

            # parallel execution of tasks
            dataset_entity_tasks = [self.opengin_service.get_entities(entity=entity) for entity in dataset_entities]
            dataset_entities = await asyncio.gather(*dataset_entity_tasks)

            # get the dataset name task
            dataset_first_datum = dataset_entities[0][0]
            dataset_name = Util.decode_protobuf_attribute_name(dataset_first_datum.name)

            # get one relation for the neighbour node to find the categoryId
            dataset_relation_instance = Relation(name="IS_ATTRIBUTE", direction="INCOMING")
            
            dataset_relations = await self.opengin_service.fetch_relation(entityId=dataset_first_datum.id, relation=dataset_relation_instance)
            
            # get the category id
            category_id = dataset_relations[0].relatedEntityId
            
            # get the metadata
            metadata_results = await self.opengin_service.get_metadata(entityId=category_id)
            
            dataset_name_decoded = Util.decode_protobuf_attribute_name(metadata_results.get(dataset_name))

            # get the dataset years
            dataset_years = [
                {
                    "datasetId": entity[0].id,
                    "year": entity[0].created.split("-")[0] if entity[0].created else "Unknown"
                }
                for entity in dataset_entities
            ]

            # sort the list by years
            dataset_years.sort(key=lambda x: x["year"])

            return {
                "name": dataset_name_decoded,
                "years": dataset_years
            }

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"failed to fetch dataset available years {e}")
            raise InternalServerError("An unexpected error occurred") from e
            
