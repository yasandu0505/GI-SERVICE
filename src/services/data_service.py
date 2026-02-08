import logging  
import asyncio
from typing import Dict
from src.exception.exceptions import InternalServerError, NotFoundError
from src.exception.exceptions import BadRequestError
from src.models.organisation_schemas import Relation
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

    async def enrich_dataset(self, dataset_dictionary: dict[str, list[str]], dataset: Entity = None, dataset_relation: Relation = None):
        """
        Enriches the dataset with the decoded name.
        
        Args:
            dataset (Entity, optional): The dataset to enrich. Defaults to None.
            dataset_relation (Relation, optional): The dataset relation to enrich. Defaults to None.
            dataset_dictionary (dict[str, list[str]], optional): The dictionary to store the enriched dataset. Defaults to None.
        Returns:
            Dataset: The enriched dataset.
        """
        try:
            if not dataset and not dataset_relation:
                raise BadRequestError("Dataset or dataset relation is required")

            if dataset_relation:
                dataset_id = dataset_relation.relatedEntityId
                datasets = await self.opengin_service.get_entities(entity=Entity(id=dataset_id))
                dataset = datasets[0]

            # decode the protobuf value
            decoded_name = Util.decode_protobuf_attribute_name(dataset.name)
            
            # remove the year from the name
            name_without_year = Util.get_name_without_year(decoded_name)
            
            # convert the name to title case
            actual_name_title_case = Util.to_title_case(name_without_year)

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

                # tasks for parallel execution
                category_enrich_tasks = [
                    self.enrich_category(category_relation=relation, categories_dictionary=categories_dictionary) 
                    for sublist in category_relations 
                    for relation in sublist
                    ]
                dataset_enrich_tasks = [
                    self.enrich_dataset(dataset_relation=relation, dataset_dictionary=dataset_dictionary)
                    for sublist in dataset_relations
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

            # decode the protobuf value
            dataset_name = Util.decode_protobuf_attribute_name(dataset_first_datum.name)

            # remove the year from the name
            name_without_year = Util.get_name_without_year(dataset_name)

            # convert the name to title case
            actual_name_title_case = Util.to_title_case(name_without_year)

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
                "name": actual_name_title_case,
                "years": dataset_years
            }

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"failed to fetch dataset available years {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def fetch_data_attributes(self, dataset_id: str):
        """
        Fetches the data attributes for a given dataset ID. Then retrieves the attributes and formats them for display based on data type.
        
        Args:
            dataset_id (str): The ID of the dataset.
        
        Returns:
            dict: A dictionary containing formatted attribute data with:
                - type: The data type ("tabular", "document", or "graph")
                - data: Type-specific data structure
                    For tabular:
                        - columns: List of column names
                        - rows: List of row data
        """

        try:
            if not dataset_id:
                raise BadRequestError("Dataset ID is required")

            # Prepare the dataset entity and relation objects
            dataset_entity = Entity(id=dataset_id)
            dataset_relation = Relation(name="IS_ATTRIBUTE", direction="INCOMING")

            # Fetch the dataset entity and relations
            dataset_entity_result, dataset_relations_result = await asyncio.gather(
                self.opengin_service.get_entities(entity=dataset_entity),
                self.opengin_service.fetch_relation(entityId=dataset_id, relation=dataset_relation)
            )

            # Extract dataset information
            if not dataset_entity_result or not dataset_relations_result:
                logger.error(f"Dataset or its relations not found for id: {dataset_id}")
                return {"message": "Dataset or its relations not found"}
            
            dataset_first_datum = dataset_entity_result[0]
            dataset_name = Util.decode_protobuf_attribute_name(dataset_first_datum.name)
            
            # Get the category id from relations
            category_id = dataset_relations_result[0].relatedEntityId
            
            attributes = await self.opengin_service.get_attributes(category_id=category_id, dataset_name=dataset_name)
            
            # transform the data for chart
            formatted_attributes = Util.transform_data_for_chart(
                attribute_data_out={"data": attributes}
            )
            
            return formatted_attributes

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"failed to fetch data attributes {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def fetch_dataset_root(self, dataset_id: str):
        """
        Fetches the root department or minister for a given dataset by traversing the category hierarchy.
        
        This function follows these steps:
        1. Gets the category ID from the dataset using IS_ATTRIBUTE relation (INCOMING)
        2. Traverses up the category hierarchy until it finds a department or minister
        
        Args:
            dataset_id (str): The ID of the dataset.
        
        Returns:
            Entity: The root entity (department or minister) for the dataset.
            
        Raises:
            BadRequestError: If dataset_id is not provided.
            NotFoundError: If no root department or minister is found.
            InternalServerError: If an unexpected error occurs.
        """
        try:
            if not dataset_id:
                raise BadRequestError("Dataset ID is required")
            
            # Fetch relation to get the category ID
            relation_instance = Relation(name="IS_ATTRIBUTE", direction="INCOMING")
            relations = await self.opengin_service.fetch_relation(
                entityId=dataset_id, 
                relation=relation_instance
            )

            if not relations:
                logger.error(f"No relation found for dataset {dataset_id}")
                raise NotFoundError("No relation found for dataset")

            category_id = relations[0].relatedEntityId
            
            # Find the root department or minister
            root_entity = await self.find_root_department_or_minister(category_id)

            if not root_entity:
                logger.error(f"Dataset not found {dataset_id}")
                return {
                    "detail": "Dataset not found"
                }

            root_entity_name = Util.decode_protobuf_attribute_name(root_entity.name)

            # arrange the response
            root_entity_data = {
                "id": root_entity.id,
                "name": root_entity_name,
                "type": root_entity.kind.minor
            }
            
            return root_entity_data

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Failed to fetch dataset root for dataset {dataset_id}: {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def fetch_dataset_categories(self, dataset_id: str):
        """
        Fetches the full category hierarchy for a given dataset.

        This function traverses from the dataset up through all parent categories
        and returns the complete path.

        Args:
            dataset_id (str): The ID of the dataset.

        Returns:
            dict: Contains dataset info and list of categories from immediate parent to root.

        Raises:
            BadRequestError: If dataset_id is not provided.
            NotFoundError: If no categories found for dataset.
            InternalServerError: If an unexpected error occurs.
        """
        try:
            if not dataset_id:
                raise BadRequestError("Dataset ID is required")

            # Get dataset entity info
            dataset_entity = Entity(id=dataset_id)
            dataset_results = await self.opengin_service.get_entities(entity=dataset_entity)

            if not dataset_results:
                raise NotFoundError(f"Dataset not found: {dataset_id}")

            dataset = dataset_results[0]
            dataset_name = Util.decode_protobuf_attribute_name(dataset.name)

            # Fetch relation to get the immediate parent category
            relation_instance = Relation(name="IS_ATTRIBUTE", direction="INCOMING")
            relations = await self.opengin_service.fetch_relation(
                entityId=dataset_id,
                relation=relation_instance
            )

            if not relations:
                logger.error(f"No category relation found for dataset {dataset_id}")
                raise NotFoundError("No category found for dataset")

            category_id = relations[0].relatedEntityId

            # Traverse up and collect all categories
            categories = await self._collect_category_hierarchy(category_id)

            return {
                "dataset": {
                    "id": dataset_id,
                    "name": dataset_name,
                    "kind": {
                        "major": dataset.kind.major,
                        "minor": dataset.kind.minor
                    }
                },
                "categories": categories
            }

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Failed to fetch categories for dataset {dataset_id}: {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def _collect_category_hierarchy(self, category_id: str) -> list:
        """
        Recursively collects all categories from the given category up to the root.

        Args:
            category_id (str): The starting category ID.

        Returns:
            list: List of category dictionaries from immediate parent to root.
        """
        categories = []
        current_id = category_id

        while current_id:
            # Fetch the category entity
            category_entity = Entity(id=current_id)
            category_results = await self.opengin_service.get_entities(entity=category_entity)

            if not category_results:
                break

            current_category = category_results[0]
            category_name = Util.decode_protobuf_attribute_name(current_category.name)

            categories.append({
                "id": current_id,
                "name": category_name,
                "kind": {
                    "major": current_category.kind.major,
                    "minor": current_category.kind.minor
                }
            })

            # Check if we've reached a department or minister (root)
            if current_category.kind and current_category.kind.minor in ["department", "minister"]:
                break

            # Get parent category
            relation_instance = Relation(name="AS_CATEGORY", direction="INCOMING")
            parent_relations = await self.opengin_service.fetch_relation(
                entityId=current_id,
                relation=relation_instance
            )

            if not parent_relations:
                break

            current_id = parent_relations[0].relatedEntityId

        return categories

    async def find_root_department_or_minister(self, category_id: str):
        """
        Recursively traverses the category hierarchy to find the root department or minister.
        
        This function checks if the current category is a department or minister. If not,
        it follows the AS_CATEGORY INCOMING relation to traverse up the hierarchy.
        
        Args:
            category_id (str): The ID of the category to check.
        
        Returns:
            Entity: The root entity with name, id and type as "department" or "minister".
        """
        try:
            if not category_id:
                raise BadRequestError("Category ID is required")
            
            # Fetch the category entity
            category_entity = Entity(id=category_id)
            category_results = await self.opengin_service.get_entities(entity=category_entity)

            current_category = category_results[0]
            
            # Check if this is a department or minister
            if current_category.kind and current_category.kind.minor in ["department", "minister"]:
                return current_category
            
            # If not, traverse up the hierarchy using AS_CATEGORY INCOMING relation
            relation_instance = Relation(name="AS_CATEGORY", direction="INCOMING")
            parent_relations = await self.opengin_service.fetch_relation(
                entityId=category_id,
                relation=relation_instance
            )

            if not parent_relations:
                logger.error(f"No parent category found for category {category_id}")
                raise NotFoundError("No parent category found for category")
            
            # Recursively check the parent category
            parent_category_id = parent_relations[0].relatedEntityId
            return await self.find_root_department_or_minister(parent_category_id)

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"Failed to find root department or minister for category {category_id}: {e}")
            raise InternalServerError("An unexpected error occurred") from e
            
