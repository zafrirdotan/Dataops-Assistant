from fastapi import APIRouter, HTTPException
import logging
from app.services.pipeline.pipeline_builder_service import PipelineBuilderService
from app.services.pipeline.deployment.dockerize_service import DockerizeService

router = APIRouter()
pipeline_builder = PipelineBuilderService()
dockerize_service = DockerizeService(logging.getLogger(__name__))

@router.post("/trigger-pipeline")
async def trigger_pipeline(pipeline_id: str):
    """
    Endpoint to trigger the pipeline build process.
    """
    try:
        # You can customize user_input and output_dir as needed
        # pipeline_id = "default pipeline input"
        result = await dockerize_service.dockerize_pipeline(pipeline_id)
        return {"status": "success", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
