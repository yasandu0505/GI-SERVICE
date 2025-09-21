from fastapi import FastAPI
from src.routers import payload_incoming_router
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

    
app.include_router(payload_incoming_router.router)
