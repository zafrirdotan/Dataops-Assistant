from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routes import chat
from app.routes import pipeline
from app.services.storage_service import MinioStorage
from app.services.database_service import get_database_service
import logging

from pipeline_builder.registry.pipeline_registry_service import getPipelineRegistryService
from app.logging_config import setup_logging

# Configure logging
logger = setup_logging(level=logging.INFO)

try:
    storage_service = MinioStorage()
    database_service = get_database_service()
    pipeline_registry_service = getPipelineRegistryService()
except Exception as e:
    logger.error(f"Error initializing services: {e}")

@asynccontextmanager
async def main(app: FastAPI):
    # Startup
    logger.info("Starting DataOps Assistant API...")
    try:
        # Initialize MinIO service and buckets
        await storage_service.initialize_pipeline_buckets()
        logger.info("MinIO service and pipeline buckets initialized successfully")
        
        # Test database connection asynchronously
        if await database_service.test_connection():
            logger.info("Database connection established successfully")
        else:
            logger.warning("Database connection test failed")
        
        yield
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
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
async def health_check():
    health_status = {
        "status": "healthy", 
        "service": "dataops-assistant",
        "components": {
            "database": "unknown",
            "storage": "unknown"
        }
    }
    
    # Check database connection
    try:
        if await database_service.test_connection():
            health_status["components"]["database"] = "healthy"
        else:
            health_status["components"]["database"] = "unhealthy"
    except Exception as e:
        health_status["components"]["database"] = "unhealthy"

    try:
        pipeline_registry_service.engine.connect()
        health_status["components"]["pipeline_registry"] = "healthy"
    except Exception as e:
        health_status["components"]["pipeline_registry"] = "unhealthy"

    # Check storage (MinIO) connection
    try:
        storage_status = await storage_service.get_storage_status()
        if storage_status.get("status") == "connected":
            health_status["components"]["storage"] = "healthy"
        else:
            health_status["components"]["storage"] = "unhealthy"
    except Exception as e:
        health_status["components"]["storage"] = "unhealthy"
    
    return health_status

# Include routers
app.include_router(chat.router, prefix="/chat", tags=["chat"])
app.include_router(pipeline.router, tags=["pipeline"])
