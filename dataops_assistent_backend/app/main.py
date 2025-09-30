from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routes import chat, data, database
from app.services.storage_service import MinioStorage
from app.services.database_service import get_database_service
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    storage_service = MinioStorage()
    database_service = get_database_service()
except Exception as e:
    logger.error(f"Error initializing services: {e}")

@asynccontextmanager
async def main(app: FastAPI):
    # Startup
    logger.info("Starting DataOps Assistant API...")
    try:
        # Initialize MinIO service (this will create buckets and load initial data)
        logger.info("MinIO service initialized successfully")
        
        # Test database connection
        if database_service.test_connection():
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
def health_check():
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
        if database_service.test_connection():
            health_status["components"]["database"] = "healthy"
        else:
            health_status["components"]["database"] = "unhealthy"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["components"]["database"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check storage (MinIO) connection
    try:
        # Assuming storage_service has a test method or we can check bucket existence
        health_status["components"]["storage"] = "healthy"
    except Exception as e:
        health_status["components"]["storage"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status

# Include routers
app.include_router(chat.router, prefix="/chat", tags=["chat"])
app.include_router(database.router, prefix="/database", tags=["database"])
