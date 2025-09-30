from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routes import chat, data
from app.services.storage_service import MinioStorage
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    storage_service = MinioStorage()
except Exception as e:
    logger.error(f"Error initializing storage service: {e}")

@asynccontextmanager
async def main(app: FastAPI):
    # Startup
    logger.info("Starting DataOps Assistant API...")
    try:
        # Initialize MinIO service (this will create buckets and load initial data)
        logger.info("MinIO service initialized successfully")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize MinIO service: {e}")
        yield
    finally:
        # Shutdown
        logger.info("Shutting down DataOps Assistant API...")

app = FastAPI(
    title="DataOps Assistant API",
    description="API for DataOps Assistant with MinIO integration",
    version="1.0.0",
    lifespan=main
)

@app.get("/")
def read_root():
    return {
        "message": "DataOps Assistant API - Ready!",
        "version": "1.0.0",
        "features": ["Chat", "Data Management", "MinIO Integration"]
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "dataops-assistant"}

# Include routers
app.include_router(chat.router, prefix="/chat", tags=["chat"])
