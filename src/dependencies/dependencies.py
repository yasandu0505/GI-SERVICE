from fastapi import Request
from src.utils import CacheService

def get_cache(request: Request) -> CacheService:
    return CacheService(request.app.state.redis)

def get_config(request: Request):
    return request.app.state.config