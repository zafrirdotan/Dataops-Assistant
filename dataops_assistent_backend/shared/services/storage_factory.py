import os
import logging
from typing import Union
from functools import lru_cache
from .storage_service import MinioStorage
from .local_storage_service import LocalStorageService


@lru_cache(maxsize=1)
def get_storage_service() -> Union[MinioStorage, LocalStorageService]:
    """
    Returns the appropriate storage service based on ENVIRONMENT variable.
    Cached using lru_cache for singleton-like behavior (thread-safe).

    Environment values:
    - 'prod' or 'production': Uses AWS S3
    - 'dev' or 'development': Uses MinIO
    - 'local-debug' or 'local': Uses LocalStorageService

    Returns:
        Storage service instance (cached, thread-safe)
    """
    environment = os.getenv("ENVIRONMENT", "dev").lower()
    print(f"Storage Factory - Environment: {environment}")
    logger = logging.getLogger("dataops")

    if environment in ["local-debug", "local"]:
        logger.info("Using LocalStorageService for local debugging")
        return LocalStorageService()

    elif environment in ["dev", "development"]:
        logger.info("Using MinioStorage for development environment")
        return MinioStorage()

    elif environment in ["prod", "production"]:
        logger.info("Using MinioStorage (S3) for production environment")
        return MinioStorage()

    else:
        logger.warning(f"Unknown environment '{environment}', defaulting to MinioStorage")
        return MinioStorage()
