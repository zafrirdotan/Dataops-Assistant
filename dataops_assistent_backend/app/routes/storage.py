"""
Storage management routes for pipeline storage and retrieval
"""

import logging
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.storage_service import MinioStorage

logger = logging.getLogger(__name__)

# Initialize the storage service
storage_service = MinioStorage()

# Create the router
router = APIRouter(prefix="/storage", tags=["storage"])


class PipelineRetrievalResponse(BaseModel):
    """Response model for pipeline retrieval"""
    pipeline_id: str
    version: str
    data: Dict[str, Any]


class PipelineVersionInfo(BaseModel):
    """Pipeline version information"""
    version: str
    created_at: str
    size: int
    path: str


class PipelineListResponse(BaseModel):
    """Response model for pipeline version listing"""
    pipeline_id: str
    versions: List[PipelineVersionInfo]


class StorageStatusResponse(BaseModel):
    """Response model for storage status"""
    status: str
    endpoint: str
    buckets: Dict[str, Any]
    total_buckets: int


@router.get("/status", response_model=StorageStatusResponse)
async def get_storage_status():
    """
    Get MinIO storage connection status and bucket information
    """
    try:
        status = await storage_service.get_storage_status()
        return StorageStatusResponse(**status)
    except Exception as e:
        logger.error(f"Error getting storage status: {e}")
        raise HTTPException(status_code=500, detail=f"Storage status check failed: {str(e)}")


@router.get("/pipelines/{pipeline_id}/versions")
async def list_pipeline_versions(pipeline_id: str) -> PipelineListResponse:
    """
    List all versions of a specific pipeline
    """
    try:
        versions = await storage_service.list_pipeline_versions(pipeline_id)
        
        if not versions:
            raise HTTPException(status_code=404, detail=f"No versions found for pipeline {pipeline_id}")
        
        version_info = [
            PipelineVersionInfo(
                version=v["version"],
                created_at=v["created_at"],
                size=v["size"],
                path=v["path"]
            )
            for v in versions
        ]
        
        return PipelineListResponse(
            pipeline_id=pipeline_id,
            versions=version_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing pipeline versions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list pipeline versions: {str(e)}")


@router.get("/pipelines/{pipeline_id}", response_model=PipelineRetrievalResponse)
async def retrieve_pipeline(
    pipeline_id: str, 
    version: Optional[str] = Query(None, description="Pipeline version. If not specified, returns latest version")
):
    """
    Retrieve a specific pipeline by ID and version
    """
    try:
        pipeline_data = await storage_service.retrieve_pipeline(pipeline_id, version)
        
        if not pipeline_data:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        # Extract version from metadata
        metadata = pipeline_data.get("metadata", {})
        pipeline_version = metadata.get("version", version or "unknown")
        
        return PipelineRetrievalResponse(
            pipeline_id=pipeline_id,
            version=pipeline_version,
            data=pipeline_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving pipeline: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve pipeline: {str(e)}")


@router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(
    pipeline_id: str,
    version: Optional[str] = Query(None, description="Pipeline version to delete. If not specified, deletes all versions")
):
    """
    Delete a pipeline or specific version
    """
    try:
        await storage_service.delete_pipeline(pipeline_id, version)
        
        if version:
            message = f"Pipeline {pipeline_id} version {version} deleted successfully"
        else:
            message = f"All versions of pipeline {pipeline_id} deleted successfully"
        
        return {"message": message}
        
    except Exception as e:
        logger.error(f"Error deleting pipeline: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete pipeline: {str(e)}")


@router.post("/initialize")
async def initialize_storage():
    """
    Initialize MinIO storage buckets for pipeline storage
    """
    try:
        await storage_service.initialize_pipeline_buckets()
        return {"message": "MinIO pipeline buckets initialized successfully"}
    except Exception as e:
        logger.error(f"Error initializing storage: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initialize storage: {str(e)}")


@router.get("/pipelines/{pipeline_id}/download/{file_type}")
async def download_pipeline_file(
    pipeline_id: str,
    file_type: str,
    version: Optional[str] = Query(None, description="Pipeline version. If not specified, uses latest version")
):
    """
    Generate a presigned URL for downloading specific pipeline files
    Available file types: code, spec, test_code, test_results, logs, requirements
    """
    try:
        # First retrieve the pipeline to get the metadata
        pipeline_data = await storage_service.retrieve_pipeline(pipeline_id, version)
        
        if not pipeline_data:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        # Get the stored file paths from metadata
        metadata = pipeline_data.get("metadata", {})
        stored_files = metadata.get("stored_files", {})
        
        if file_type not in stored_files:
            raise HTTPException(status_code=404, detail=f"File type '{file_type}' not found for pipeline {pipeline_id}")
        
        # Parse the S3 path to get bucket and object key
        s3_path = stored_files[file_type]
        bucket, object_key = storage_service._parse_s3_path(s3_path)
        
        # Generate presigned URL for download
        download_url = storage_service.generate_presigned_url(
            bucket=bucket,
            object_name=object_key,
            expires=3600  # 1 hour
        )
        
        return {
            "download_url": download_url,
            "file_type": file_type,
            "pipeline_id": pipeline_id,
            "version": metadata.get("version", "unknown"),
            "expires_in": 3600
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating download URL: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate download URL: {str(e)}")


# Health check specific to storage
@router.get("/health")
async def storage_health_check():
    """
    Storage-specific health check
    """
    try:
        status = await storage_service.get_storage_status()
        
        if status.get("status") == "connected":
            return {
                "status": "healthy",
                "storage": status,
                "message": "Storage service is operational"
            }
        else:
            return {
                "status": "unhealthy",
                "storage": status,
                "message": "Storage service connection issues"
            }
            
    except Exception as e:
        logger.error(f"Storage health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "message": "Storage service health check failed"
        }