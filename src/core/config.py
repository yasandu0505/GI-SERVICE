from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    BASE_URL_QUERY: str
    HTTP_POOL_SIZE: int = 50 
    HTTP_POOL_SIZE_PER_HOST: int = 40
    HTTP_TTL_DNS_CACHE: int = 300
    HTTP_TIMEOUT_TOTAL: int = 90
    HTTP_TIMEOUT_CONNECT: int = 30
    HTTP_TIMEOUT_SOCK_CONNECT: int = 30
    HTTP_TIMEOUT_SOCK_READ: int = 90
    THROTTLING_MAX_CONCURRENT: int = 200
    THROTTLING_TIMEOUT: int = 30

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()