from fastapi import FastAPI
from src.routers import payload_incoming_router

app = FastAPI(
    title="GI - Service",     
    description="API Adapter to the OpenGIn (Open Genaral Information Network)",
    version="1.0.0"
)

app.include_router(payload_incoming_router.router)
