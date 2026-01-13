from src.exception.exceptions import NotFoundError
from src.exception.exceptions import InternalServerError
from src.exception.exceptions import BadRequestError
from src.models.organisation_schemas import Label
from src.models.organisation_schemas import Dataset
from src.models.organisation_schemas import Relation
from src.models.organisation_schemas import Category
from src.utils.util_functions import Util
from src.models.organisation_schemas import Kind
from src.models.organisation_schemas import Entity
import logging  
import asyncio
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

    @property
    def session(self) -> ClientSession:
        """Access the global session"""
        return http_client.session

    async def enrich_dataset(self,  category_id: str, dataset: Entity = None, dataset_relation: Relation = None):
        """
        Enriches the dataset with the decoded name.
        
        Args:
            category_id (str): The ID of the parent category to the dataset.
            dataset (Entity, optional): The dataset to enrich. Defaults to None.
            dataset_relation (Relation, optional): The dataset relation to enrich. Defaults to None.
        
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
                datasets = await self.opengin_service.get_entity(entity=Entity(id=dataset_id))
                dataset = datasets[0]

            decoded_name = Util.decode_protobuf_attribute_name(dataset.name)
            metadata = await self.opengin_service.get_metadata(category_id)

            if metadata:
                actual_name = Util.decode_protobuf_attribute_name(metadata.get(decoded_name))
            else:
                actual_name = "Dataset Name is not provided"

            updated_dataset = Dataset(id=dataset.id, label=Label(id=decoded_name,name=actual_name), kind=dataset.kind, parentId=category_id)
            return updated_dataset

        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to enrich dataset {e}")
            raise InternalServerError("An unexpected error occurred") from e


    async def enrich_category(self, category: Entity = None, category_relation: Relation = None):
        """
        Enriches the category with the decoded name.
        
        Args:
            category (Entity, optional): The category to enrich. Defaults to None.
            category_relation (Relation, optional): The category relation to enrich. Defaults to None.
        
        Returns:
            Category: The enriched category.
        """

        try:
            if not category and not category_relation:
                raise BadRequestError("No category or category relation provided")
            
            if category_relation:
                category_id = category_relation.relatedEntityId
                categories = await self.opengin_service.get_entity(entity=Entity(id=category_id))
                category = categories[0]

            decoded_name = Util.decode_protobuf_attribute_name(category.name)
            updated_category = Category(id=category.id, name=decoded_name, kind=category.kind)
            return updated_category
        
        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to enrich category {e}")
            raise InternalServerError("An unexpected error occurred") from e


    async def fetch_data_catalog(self, parent_id: str = None):
        """
        Fetches the data catalog for a given parent ID. If no parent ID is provided, it fetches the parent categories. Otherwise it fetches the child categories and datasets for the given parent ID.
        
        Args:
            parent_id (str, optional): The ID of the parent entity. Defaults to None.
        
        Returns:
            dict: A dictionary containing the list of categories and datasets.
        """
        
        try:
            if not parent_id:
                entity = Entity(kind=Kind(major="Category", minor="parentCategory"))
                parentCategories = await self.opengin_service.get_entity(entity=entity)

                enrich_category_task = [self.enrich_category(category) for category in parentCategories]
                parent_categories = await asyncio.gather(*enrich_category_task)

                return {
                    "categories": parent_categories,
                    "datasets": []
                    }
            
            else:
                relations = [
                    Relation(name="AS_CATEGORY", direction="OUTGOING"),
                    Relation(name="IS_ATTRIBUTE", direction="OUTGOING")
                ]
                
                fetch_relation_tasks = [self.opengin_service.fetch_relation(entityId=parent_id, relation=relation) for relation in relations]
                category_relations, dataset_relations = await asyncio.gather(*fetch_relation_tasks)

                category_enrich_tasks = [self.enrich_category(category_relation=relation) for relation in category_relations]
                dataset_enrich_tasks = [self.enrich_dataset(dataset_relation=relation, category_id=parent_id) for relation in dataset_relations]
                
                category_results, dataset_results = await asyncio.gather(
                    asyncio.gather(*category_enrich_tasks),
                    asyncio.gather(*dataset_enrich_tasks)
                )

                return {
                    "categories": category_results if category_results else [],
                    "datasets": dataset_results if dataset_results else []
                }
                
        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to fetch data catalog {e}")
            raise InternalServerError("An unexpected error occurred") from e

    async def enrich_dataset_years(self, dataset_relation: Relation):
        try:
            if not dataset_relation:
                raise BadRequestError("Dataset relation is required")
            
            dataset_start_time = dataset_relation.startTime.split("-")[0]

            return {
                "datasetId": dataset_relation.relatedEntityId,
                "year": dataset_start_time
            }
            
        except (BadRequestError):
            raise
        except Exception as e:
            logger.error(f"failed to enrich dataset years {e}")
            raise InternalServerError("An unexpected error occurred") from e
    
    async def fetch_dataset_available_years(self, category_id: str):
        try:
            if not category_id:
                raise BadRequestError("Category ID is required")

            relation = Relation(name="IS_ATTRIBUTE", direction="OUTGOING")

            dataset_relations = await self.opengin_service.fetch_relation(entityId=category_id, relation=relation)
    
            if not dataset_relations:
                raise NotFoundError("No datasets found")

            # get the dataset name
            dataset_name_task = 
            
            enrich_dataset_year_tasks = [self.enrich_dataset_years(dataset_relation=relation) for relation in dataset_relations]
            dataset_years = await asyncio.gather(*enrich_dataset_year_tasks)

            return {
                "name": dataset_name,
                "year": dataset_years
            }

        except (BadRequestError, NotFoundError):
            raise
        except Exception as e:
            logger.error(f"failed to fetch dataset available years {e}")
            raise InternalServerError("An unexpected error occurred") from e
            
