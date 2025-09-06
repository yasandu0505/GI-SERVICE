from fastapi import FastAPI
from src.routers import payload_incoming_router
from src.utils import prepare_cache

app = FastAPI(
    title="GI - Service",     
    description="API Adapter to the OpenGIn (Open Genaral Information Network)",
    version="1.0.0"
)

# Global cache dictionary
CACHE = {}

@app.on_event("startup")
async def startup_event():
    print("🚀 App starting... preloading cache")
    CACHE["ministries"] = await prepare_cache("Organisation", "minister")  
    CACHE["departments"] = await prepare_cache("Organisation", "department") 
    # CACHE["departments"] = await prepare_cache("People", "citizen") 
    print("✅ Cache preloaded")
    print(CACHE)


app.include_router(payload_incoming_router.router)
