from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    BASE_URL_QUERY: str
    BASE_URL_CRUD: str | None = None
    MONGODB_URI: str | None = None

    class Config:
        env_file = ".env"

settings = Settings()