from fastapi import APIRouter, HTTPException
import logging
from app.services.pipeline.pipeline_builder_service import PipelineBuilderService
from app.services.pipeline.deployment.dockerize_service import DockerizeService
from app.services.pipeline.registry.pipeline_registry_service import getPipelineRegistryService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter()
pipeline_builder = PipelineBuilderService()
dockerize_service = DockerizeService(logging.getLogger(__name__))
pipeline_registry = getPipelineRegistryService()

@router.post("/trigger-pipeline")
async def trigger_pipeline(pipeline_id: str):
    """
    Endpoint to trigger the pipeline build process.
    """
    try:
        # You can customize user_input and output_dir as needed
        # pipeline_id = "default pipeline input"
        result = await dockerize_service.dockerize_pipeline(pipeline_id)
        logger.info(f"Pipeline {pipeline_id} triggered successfully.")
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Error triggering pipeline {pipeline_id}: {str(e)}")
        return {"status": "error", "message": str(e)}

@router.get("/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    pipeline = await pipeline_registry.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    # If using SQLAlchemy model, convert to dict
    return pipeline.__dict__ if hasattr(pipeline, "__dict__") else pipeline

@router.get("/pipelines")
async def get_pipelines():
    pipelines = await pipeline_registry.list_pipelines()
    # Convert each to dict if needed
    return [p.__dict__ if hasattr(p, "__dict__") else p for p in pipelines]
