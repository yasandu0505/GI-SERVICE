from fastapi import FastAPI
from src.routers import payload_incoming_router
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware




app = FastAPI(
    title="GI - Service",     
    description="API Adapter to the OpenGIn (Open Genaral Information Network)",
    version="1.0.0"
)

# Load config at startup
@app.on_event("startup")
async def load_config():
    load_dotenv()
    
    BASE_URL_CRUD = os.getenv("BASE_URL_CRUD")
    BASE_URL_QUERY = os.getenv("BASE_URL_QUERY")
    MONGODB_URI = os.getenv("MONGODB_URI")
    
    if BASE_URL_CRUD and BASE_URL_QUERY and MONGODB_URI:
        print(f"✅ BASE_URL_CRUD and BASE_URL_QUERY and MONGODB_URI found: {BASE_URL_QUERY} , {BASE_URL_CRUD} , {MONGODB_URI}...")
        app.state.config = {
            "BASE_URL_CRUD": BASE_URL_CRUD,
            "BASE_URL_QUERY": BASE_URL_QUERY,
            "MONGODB_URI": MONGODB_URI
        }      
    else:
        print("❌ not found in environment")
        print("Available env vars:", list(os.environ.keys()))
        
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

        
app.include_router(payload_incoming_router.router)
