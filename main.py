from fastapi import FastAPI
from src.routers import payload_incoming_router

app = FastAPI()

app.include_router(payload_incoming_router.router)
