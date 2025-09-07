import json
from redis.asyncio import Redis
from typing import Optional

class CacheService:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def get(self, key: str):
        data = await self.redis.get(key)
        return json.loads(data) if data else None

    async def set(self, key: str, value, expire: Optional[int] = None):
        return await self.redis.set(key, json.dumps(value), ex=expire)

    # Custom convenience methods
    async def get_ministries(self):
        return await self.get("ministries")

    async def get_departments(self):
        return await self.get("departments")
