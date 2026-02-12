from fastapi import FastAPI
from src.routers import payload_incoming_router
from src.routers import organisation_router, data_router, search_router, person_router
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware
from src.utils.http_client import http_client
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

# Load config at startup
def load_config():
    load_dotenv()
    
    BASE_URL_CRUD = os.getenv("BASE_URL_CRUD")
    BASE_URL_QUERY = os.getenv("BASE_URL_QUERY")
    
    if BASE_URL_CRUD and BASE_URL_QUERY:
        logger.info(f"BASE_URL_CRUD and BASE_URL_QUERY found: {BASE_URL_QUERY} , {BASE_URL_CRUD}...")
        return {
            "BASE_URL_CRUD": BASE_URL_CRUD,
            "BASE_URL_QUERY": BASE_URL_QUERY
        }      
    else:
        logger.error("Environment variables not found")
        raise RuntimeError("Missing required environment variables")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.config = load_config()
    logger.info(f"Config set: {app.state.config}")

    await http_client.start()

    yield

    await http_client.close()

app = FastAPI(
    title="GI - Service",     
    description="API Adapter to the OpenGIn (Open General Information Network)",
    version="1.0.0",
    lifespan=lifespan
)
        
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # or ["*"] for all
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(payload_incoming_router.router)
app.include_router(organisation_router)
app.include_router(data_router)
app.include_router(search_router)
app.include_router(person_router)
