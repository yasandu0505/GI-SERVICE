from fastapi import FastAPI
from src.routers import payload_incoming_router
from src.utils import prepare_cache, CacheService
import redis.asyncio as redis
import json

app = FastAPI(
    title="GI - Service",     
    description="API Adapter to the OpenGIn (Open Genaral Information Network)",
    version="1.0.0"
)


@app.on_event("startup")
async def startup_event():
    print("🚀 App starting... connecting to Redis")

    app.state.redis = redis.from_url("redis://localhost:6379", decode_responses=True)
    
    ministries = await prepare_cache("Organisation", "minister")
    departments = await prepare_cache("Organisation", "department")
    # CACHE["departments"] = await prepare_cache("People", "citizen") 
    
    await app.state.redis.set("ministries", json.dumps(ministries))
    await app.state.redis.set("departments", json.dumps(departments))
    print("✅ Cache preloaded into Redis")

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.redis.close()
    print("🛑 Redis connection closed")
    
app.include_router(payload_incoming_router.router)
