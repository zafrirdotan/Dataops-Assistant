import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Settings:
    # Database settings
    database_url: str
    database_host: str
    database_port: int
    database_name: str
    database_user: str
    database_password: str
    
    # MinIO settings
    minio_host: str
    minio_port: int
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    
    # Data paths
    data_directory: str
    csv_data_path: str

def get_settings() -> Settings:
    """Load settings from environment variables"""
    return Settings(
        # Database
        database_url=os.getenv("DATABASE_URL", "postgresql://localhost:5432/dataops"),
        database_host=os.getenv("DATABASE_HOST", "localhost"),
        database_port=int(os.getenv("DATABASE_PORT", "5432")),
        database_name=os.getenv("DATABASE_NAME", "dataops"),
        database_user=os.getenv("DATABASE_USER", "postgres"),
        database_password=os.getenv("DATABASE_PASSWORD", "password"),
        
        # MinIO
        minio_host=os.getenv("MINIO_HOST", "localhost"),
        minio_port=int(os.getenv("MINIO_PORT", "9000")),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        minio_secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        
        # Data paths
        data_directory=os.getenv("DATA_DIRECTORY", "/data"),
        csv_data_path=os.getenv("CSV_DATA_PATH", "/data/csv")
    )
