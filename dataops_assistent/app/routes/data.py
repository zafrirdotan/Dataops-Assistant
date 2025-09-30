from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
from app.services.storage_service import MinioStorage
import os
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

try:
    storage_service = MinioStorage()
except Exception as e:
    logger.error(f"Error initializing storage service: {e}")

class DataUploadResponse(BaseModel):
    message: str
    object_key: str
    public_url: str

@router.post("/upload", response_model=DataUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """Upload a file to MinIO storage"""
    try:
        contents = await file.read()
        result = storage_service.direct_put_bytes(
            f"uploads/{file.filename}", 
            contents, 
            file.content_type
        )
        
        return DataUploadResponse(
            message=f"File {file.filename} uploaded successfully",
            object_key=result["object_key"],
            public_url=result["public_url"]
        )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/initialize")
async def initialize_data():
    """Initialize MinIO with data from the data folder"""
    try:
        uploaded_files = []
        data_dir = "/app/data"
        
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.endswith(('.csv', '.json', '.parquet', '.xlsx')):
                    file_path = os.path.join(data_dir, filename)
                    with open(file_path, 'rb') as f:
                        data = f.read()
                    
                    result = storage_service.direct_put_bytes(
                        f'initial-data/{filename}', 
                        data,
                        'application/octet-stream'
                    )
                    uploaded_files.append({
                        "filename": filename,
                        "object_key": result["object_key"],
                        "public_url": result["public_url"]
                    })
                    logger.info(f'Uploaded {filename} to MinIO')
        
        return {
            "message": "Data initialization completed",
            "uploaded_files": uploaded_files,
            "total_files": len(uploaded_files)
        }
    except Exception as e:
        logger.error(f"Error initializing data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list")
async def list_files():
    """List all files in the MinIO bucket"""
    try:
        # This is a simple implementation - you might want to extend it
        # to use boto3 list_objects_v2 for a proper file listing
        return {"message": "File listing endpoint - implement with boto3 list_objects_v2"}
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/presigned-url/{object_key}")
async def get_presigned_url(object_key: str):
    """Get a presigned URL for downloading a file"""
    try:
        result = storage_service.presigned_get(object_key)
        return result
    except Exception as e:
        logger.error(f"Error getting presigned URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))
