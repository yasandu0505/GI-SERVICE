from fastapi import FastAPI
from src.routers import organisation_router, data_router, search_router, person_router
from fastapi.middleware.cors import CORSMiddleware
from src.middleware.throttling import ThrottlingMiddleware
from src.utils.http_client import http_client
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
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

app.add_middleware(ThrottlingMiddleware)

app.include_router(organisation_router)
app.include_router(data_router)
app.include_router(search_router)
app.include_router(person_router)
