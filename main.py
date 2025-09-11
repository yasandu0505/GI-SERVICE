from fastapi import FastAPI
from src.routers import payload_incoming_router
from src.utils import prepare_cache, CacheService
import redis.asyncio as redis
# import json
import yaml

app = FastAPI(
    title="GI - Service",     
    description="API Adapter to the OpenGIn (Open Genaral Information Network)",
    version="1.0.0"
)

# Load config at startup
@app.on_event("startup")
async def load_config():
    with open("config.yaml", "r") as f:
        app.state.config = yaml.safe_load(f)


# @app.on_event("startup")
# async def startup_event():
#     print("🚀 App starting... connecting to Redis")

#     app.state.redis = redis.from_url("redis://localhost:6379", decode_responses=True)
    
#     ministries = await prepare_cache("Organisation", "minister")
#     print("✅ ministries preloaded into Redis")
#     departments = await prepare_cache("Organisation", "department")
#     print("✅ departments preloaded into Redis")
#     people = await prepare_cache("Person", "citizen")
#     print("✅ people preloaded into Redis")
    
#     await app.state.redis.set("ministries", json.dumps(ministries))
#     await app.state.redis.set("departments", json.dumps(departments))
#     await app.state.redis.set("people", json.dumps(people))
    
#     print("✅ Cache preloaded into Redis - Complete")

# @app.on_event("shutdown")
# async def shutdown_event():
#     await app.state.redis.close()
#     print("🛑 Redis connection closed")


    
app.include_router(payload_incoming_router.router)
