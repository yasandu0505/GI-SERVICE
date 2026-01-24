from pydantic import BaseModel, Field

class DataCatalogRequest(BaseModel):
    categoryIds: list[str] = Field(None, description="List of category IDs")

class DatasetYearsRequest(BaseModel):
    datasetIds: list[str] = Field(None, description="List of dataset IDs")