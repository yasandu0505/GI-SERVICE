from fastapi import FastAPI
from src.routers import payload_incoming_router
from src.routers.organisation_v1_router import router as organisation_v1_router
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware
from src.utils.http_client import http_client
from contextlib import asynccontextmanager

# Load config at startup
def load_config():
    load_dotenv()
    
    BASE_URL_CRUD = os.getenv("BASE_URL_CRUD")
    BASE_URL_QUERY = os.getenv("BASE_URL_QUERY")
    MONGODB_URI = os.getenv("MONGODB_URI")
    
    if BASE_URL_CRUD and BASE_URL_QUERY and MONGODB_URI:
        print(f"BASE_URL_CRUD and BASE_URL_QUERY and MONGODB_URI found: {BASE_URL_QUERY} , {BASE_URL_CRUD} , {MONGODB_URI}...")
        return {
            "BASE_URL_CRUD": BASE_URL_CRUD,
            "BASE_URL_QUERY": BASE_URL_QUERY,
            "MONGODB_URI": MONGODB_URI,
        }      
    else:
        print("Environment variables not found")
        print("Available env vars:", list(os.environ.keys()))
        raise RuntimeError("Missing required environment variables")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.config = load_config()
    print(f"Config set: {app.state.config}")

    await http_client.start()
    print('HTTP client started')

    yield

    await http_client.close()
    print('HTTP client closed')

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
app.include_router(organisation_v1_router)
