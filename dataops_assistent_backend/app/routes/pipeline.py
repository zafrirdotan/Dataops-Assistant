from fastapi import APIRouter, HTTPException, Depends
import logging
from pipeline_builder.pipeline_builder_service import PipelineBuilderService
from pipeline_builder.deployment.dockerize_service import DockerizeService
from pipeline_builder.registry.pipeline_registry_service import getPipelineRegistryService
from app.core.deps import get_optional_current_user
from shared.models.user import User
from shared.models.pipeline_types import Pipeline
from typing import Optional

logger = logging.getLogger("dataops")

router = APIRouter()
pipeline_builder = PipelineBuilderService()
dockerize_service = DockerizeService(logger)
pipeline_registry = getPipelineRegistryService()

@router.post("/trigger-pipeline")
async def trigger_pipeline(pipeline_id: str):
    """
    Endpoint to trigger the pipeline build process.
    """
    try:
        # You can customize user_input and output_dir as needed
        # pipeline_id = "default pipeline input"
        pipeline: Pipeline | None = await pipeline_registry.get_pipeline(pipeline_id)
        if not pipeline or not pipeline.image_id:
            raise HTTPException(status_code=404, detail="Pipeline image not found")

        result = await dockerize_service.run_pipeline_in_container(pipeline.image_id)
        logger.info(f"Pipeline {pipeline_id} triggered successfully.")
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Error triggering pipeline {pipeline_id}: {str(e)}")
        return {"status": "error", "message": str(e)}

@router.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str, current_user: Optional[User] = Depends(get_optional_current_user)) -> Pipeline:
    pipeline: Pipeline | None = await pipeline_registry.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline

@router.get("/pipelines")
async def get_pipelines(current_user: Optional[User] = Depends(get_optional_current_user)) -> list[Pipeline]:
    """Get all pipelines (optionally filtered by user if authenticated)."""
    return await pipeline_registry.list_pipelines()
