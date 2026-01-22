from pydantic import BaseModel, Field

class DataCatalogRequest(BaseModel):
    categoryIds: list[str] = Field(None, description="List of category IDs")