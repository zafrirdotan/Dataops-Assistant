

import logging
import docker 
import os
import aiofiles
import shutil

from ..deployment.pipeline_output_service import PipelineOutputService
class DockerizeService:
    """Service to dockerize pipeline deployments."""
    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()
        self.docker_client = docker.from_env()
        self.network_name = "dataops-assistant_default"

    async def dockerize_pipeline(self, pipeline_id: str) -> dict:
        # Step 1: Retrieve pipeline files from MinIO
        try:
            stored_files = await self.output_service.get_pipeline_files(pipeline_id)
            if not stored_files:
                self.log.error(f"No files found for pipeline ID: {pipeline_id}")
                return {"success": False, "details": "No files found for the given pipeline ID."}
        except Exception as e:
            self.log.error(f"Failed to retrieve pipeline files: {e}")
            return {"success": False, "details": f"Failed to retrieve pipeline files: {e}"}
        
        self.log.info(f"Retrieved files for pipeline ID: {pipeline_id}")
        # Step 2: Save files to a temporary build context
        try:
            build_dir = f"/tmp/pipeline_builds/{pipeline_id}"
            os.makedirs(build_dir, exist_ok=True)
            try:
                pipeline_file = os.path.join(build_dir, "pipeline.py")
                requirements_file = os.path.join(build_dir, "requirements.txt")
                env_file = os.path.join(build_dir, ".env")
                dockerfile = os.path.join(build_dir, "Dockerfile")

                # debug stored_files content data
                # self.log.info(f"Stored files content keys: {list(stored_files.keys())}")
                # self.log.info(f"Stored files content: {stored_files}")
                self.log.info(f"Stored .env content: {stored_files.get('.env')}")


                async with aiofiles.open(pipeline_file, 'w') as f:
                    await f.write(stored_files.get('code', ''))

                async with aiofiles.open(requirements_file, 'w') as f:
                    await f.write(stored_files.get('requirements', ''))

                async with aiofiles.open(env_file, 'w') as f:
                    await f.write(stored_files.get('.env', ''))

                async with aiofiles.open(dockerfile, 'w') as f:
                    await f.write(stored_files.get('dockerfile', ''))

                self.log.info(f"Pipeline files saved to build context: {build_dir}")
            except Exception as e:
                self.log.error(f"Failed to save pipeline files to build context: {e}")
                return {"success": False, "details": f"Failed to save pipeline files: {e}"}
            

            # Step 3: Build Docker image
            try:
                image_tag = f"pipeline-{pipeline_id}:latest"
                container_name = f"pipeline_{pipeline_id}_container"

                image, logs = self.docker_client.images.build(
                    path=build_dir,
                    tag=image_tag,
                    rm=True, 
                    forcerm=True,
                    pull=False,
                    )
                for log in logs:
                        self.log.info(log)
                self.log.info(f"Docker image built successfully for pipeline ID: {pipeline_id}")
            except Exception as e:
                self.log.error(f"Failed to build Docker image: {e}")
                return {"success": False, "details": f"Failed to build Docker image: {e}"}
        
            # Step 4: Run the container and connect it to the network
            try:

                host_data_path = os.getenv("HOST_DATA_PATH", "/Users/yourusername/project/data")
                host_output_path = os.getenv("HOST_OUTPUT_PATH", "/Users/yourusername/project/output")

                container = self.docker_client.containers.run(
                    image_tag,
                    detach=True,
                    network=self.network_name,
                    name=container_name,
                    volumes={
                            os.path.abspath(host_data_path): {"bind": "/app/data", "mode": "ro"},
                            os.path.abspath(host_output_path): {"bind": "/app/output", "mode": "rw"},
                        },
                    # remove=True,
                    labels={"app": "dataops-assistant", "pipeline_id": pipeline_id}, 
                    # auto_remove=True

                )

                self.docker_client.containers.prune(filters={"label": "app=dataops-assistant"})

                self.log.info(f"Docker container started for pipeline ID: {pipeline_id}, Container ID: {container.id}")
            except Exception as e:
                self.log.error(f"Failed to start Docker container: {e}")
                return {"success": False, "details": f"Failed to start Docker container: {e}"}
            return {"success": True, "container_id": container.id}
        finally:
            # Clean up build directory
            try:
                if os.path.exists(build_dir):
                    shutil.rmtree(build_dir)
                    self.log.info(f"Cleaned up build directory: {build_dir}")
            except Exception as cleanup_error:
                self.log.error(f"Failed to clean up build directory: {cleanup_error}")

