from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    BASE_URL_QUERY: str = "http://localhost:8081"
    BASE_URL_CRUD: str | None = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()