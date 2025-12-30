import docker 
import os
import aiofiles
import shutil
import asyncio
import json

from ..deployment.pipeline_output_service import PipelineOutputService

class DockerizeService:
    """Service to dockerize pipeline deployments."""
    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()
        self.docker_client = docker.from_env()
        self.network_name = "dataops-assistant-net"
        self.env_test_template_path = os.path.join(os.path.dirname(__file__), "../testing/.env.test_template")
        self.host_data_path = os.getenv("HOST_DATA_PATH", "/Users/yourusername/project/data")
        self.host_output_path = os.getenv("HOST_OUTPUT_PATH", "/Users/yourusername/project/output")

    async def dockerize_pipeline(self, pipeline_id: str, is_test: bool = False) -> dict:
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

                async with aiofiles.open(pipeline_file, 'w') as f:
                    await f.write(stored_files.get('pipeline', ''))

                async with aiofiles.open(requirements_file, 'w') as f:
                    await f.write(stored_files.get('requirements', ''))

                async with aiofiles.open(dockerfile, 'w') as f:
                    await f.write(stored_files.get('dockerfile', ''))
                if not is_test:
                    async with aiofiles.open(env_file, 'w') as f:
                        await f.write(stored_files.get('.env', ''))
                elif is_test:
                    with open(self.env_test_template_path, "r") as f:
                        env_test_content = f.read()
                    async with aiofiles.open(env_file, 'w') as f:
                        await f.write(env_test_content)

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
                    if 'stream' in log:
                        self.log.debug(log['stream'].strip())
                    else:
                        self.log.debug(log)
                self.log.info(f"Docker image built successfully for pipeline ID: {pipeline_id}")
            except Exception as e:
                self.log.error(f"Failed to build Docker image: {e}")
                return {"success": False, "details": f"Failed to build Docker image: {e}"}
        
            # Step 4: Run the container and connect it to the network
            try:
                container = await asyncio.to_thread(
                    self.docker_client.containers.run,
                    image_tag,
                    detach=True,
                    network=self.network_name,
                    name=container_name,
                    volumes={
                        os.path.abspath(self.host_data_path): {"bind": "/app/data", "mode": "ro"},
                        os.path.abspath(self.host_output_path): {"bind": "/app/output", "mode": "rw"},
                    },
                    labels={"app": "dataops-assistant", "pipeline_id": pipeline_id},
                )

                self.log.info(f"Docker container started for pipeline ID: {pipeline_id}, Container ID: {container.id}")

                # Remove the container and image after running
                await asyncio.to_thread(container.wait)
                logs = await asyncio.to_thread(container.logs)
                logs = logs.decode('utf-8')
                self.log.info(f"Container logs:\n{logs}")

                await asyncio.to_thread(container.remove, force=True)
                await asyncio.to_thread(self.docker_client.images.remove, image=image_tag, force=True)

                await asyncio.to_thread(self.docker_client.containers.prune, filters={"label": "app=dataops-assistant"})
                self.log.info(f"Cleaned up container and image for pipeline ID: {pipeline_id}")

            except Exception as e:
                self.log.error(f"Failed to start Docker container: {e}")
                return {"success": False, "details": f"Failed to start Docker container: {e}"}
            return {"success": True}
        finally:
            # Clean up build directory
            try:
                if os.path.exists(build_dir):
                    shutil.rmtree(build_dir)
                    self.log.info(f"Cleaned up build directory: {build_dir}")
            except Exception as cleanup_error:
                self.log.error(f"Failed to clean up build directory: {cleanup_error}")

    async def build_and_test_docker_image(self, pipeline_id: str) -> dict:
        """Wrapper method to build and dockerize the pipeline."""
        result = await self.dockerize_pipeline(pipeline_id, is_test=True)
        return result

    async def test_pipeline_in_docker(self, pipeline_id: str) -> dict:
        """
        Use the test-runner image to run tests for the given pipeline_id.
        Copies pipeline files to a temp dir, mounts it into the test-runner container, and runs pytest.
        """
        import tempfile
        try:
            stored_files = await self.output_service.get_pipeline_files(pipeline_id)
            if not stored_files:
                self.log.error(f"No files found for pipeline ID: {pipeline_id}")
                return {"success": False, "details": "No files found for the given pipeline ID."}
        except Exception as e:
            self.log.error(f"Failed to retrieve pipeline files: {e}")
            return {"success": False, "details": f"Failed to retrieve pipeline files: {e}"}

        self.log.info(f"Retrieved files for pipeline ID: {pipeline_id}")

        # Use a named Docker volume 'pipeline-test' for /app/pipeline
        try:
            # Ensure the volume exists
            volume_name = "pipeline-test"
            try:
                self.docker_client.volumes.get(volume_name)
            except docker.errors.NotFound:
                self.docker_client.volumes.create(name=volume_name)

            import tempfile
            from shared.copy_to_volume import copy_to_volume
            # Create a temp dir and write the files to it
            with tempfile.TemporaryDirectory() as temp_dir:
                with open(os.path.join(temp_dir, "pipeline.py"), "w") as f:
                    f.write(stored_files.get('pipeline', ''))
                with open(os.path.join(temp_dir, "test.py"), "w") as f:
                    f.write(stored_files.get('test_code', ''))
                with open(os.path.join(temp_dir, "requirements.txt"), "w") as f:
                    f.write(stored_files.get('requirements', ''))
                with open(os.path.join(temp_dir, ".env"), "w") as f:
                    f.write(open(self.env_test_template_path, "r").read())
                # Copy all files in temp_dir to the volume
                copy_to_volume(volume_name, temp_dir, dest_path="/app/pipeline")
            self.log.info(f"Pipeline test files written to Docker volume: {volume_name}")
        except Exception as e:
            self.log.error(f"Failed to write pipeline test files to Docker volume: {e}")
            return {"success": False, "details": f"Failed to write pipeline test files to Docker volume: {e}"}

        # Run the test-runner container with the named volume mounted
        try:
            container = await asyncio.to_thread(
                self.docker_client.containers.run,
                image="dataops-assistant-test-runner:latest",
                command=["pytest", "/app/pipeline/test.py"],
                volumes={
                    volume_name: {"bind": "/app/pipeline", "mode": "rw"}
                },
                working_dir="/app/pipeline",
                detach=True,
                network=self.network_name,
                labels={"app": "dataops-assistant", "pipeline_id": pipeline_id},
            )
            self.log.info(f"Started test-runner container for pipeline ID: {pipeline_id}, Container ID: {container.id}")
            await asyncio.to_thread(container.wait)
            logs = await asyncio.to_thread(container.logs)
            logs = logs.decode('utf-8')
            self.log.info(f"Test-runner container logs:\n{logs}")
            exit_code = container.attrs['State']['ExitCode']
            await asyncio.to_thread(container.remove, force=True)
            if exit_code == 0:
                return {"success": True, "details": "All tests passed", "logs": logs}
            else:
                return {"success": False, "details": "Tests failed", "logs": logs}
        except Exception as e:
            self.log.error(f"Failed to run test-runner container: {e}")
            return {"success": False, "details": f"Failed to run test-runner container: {e}"}

    async def dockerize_pipeline_v2(self, pipeline_id: str) -> dict:
        """
        Build and start a pipeline container, returning the container ID. Reuses build context if it exists.
        """
        import aiofiles
        build_dir = f"/tmp/pipeline_builds/{pipeline_id}"
        os.makedirs(build_dir, exist_ok=True)
        try:
            stored_files = await self.output_service.get_pipeline_files(pipeline_id)
            if not stored_files:
                self.log.error(f"No files found for pipeline ID: {pipeline_id}")
                return {"success": False, "details": "No files found for the given pipeline ID."}
        except Exception as e:
            self.log.error(f"Failed to retrieve pipeline files: {e}")
            return {"success": False, "details": f"Failed to retrieve pipeline files: {e}"}

        # Write pipeline files to build context
        pipeline_file = os.path.join(build_dir, "pipeline.py")
        dockerfile_path = os.path.join(build_dir, "Dockerfile")
        metadata_file = os.path.join(build_dir, "metadata.json")   

        async with aiofiles.open(pipeline_file, 'w') as f:
            await f.write(stored_files.get('pipeline', ''))
        # Write metadata file
        async with aiofiles.open(metadata_file, 'w') as f:
            await f.write(json.dumps(stored_files.get('metadata', '')))

        # Generate Dockerfile from template
        template_path = os.path.join(os.path.dirname(__file__), "Dockerfile.template")
        with open(template_path, "r") as tpl:
            dockerfile_content = tpl.read()
        with open(dockerfile_path, "w") as df:
            df.write(dockerfile_content)

        # Build the Docker image
        image_tag = f"pipeline-{pipeline_id}:latest"
        try:
            image, logs = self.docker_client.images.build(
                path=build_dir,
                tag=image_tag,
                rm=True,
                forcerm=True,
                pull=False,
            )
            for log in logs:
                if 'stream' in log:
                    self.log.debug(log['stream'].strip())
                else:
                    self.log.debug(log)
            self.log.info(f"Docker image built successfully for pipeline ID: {pipeline_id}")
        except Exception as e:
            self.log.error(f"Failed to build Docker image: {e}")
            return {"success": False, "details": f"Failed to build Docker image: {e}"}

        # Start the container and return its ID
   
        container_name = f"pipeline_{pipeline_id}_container"
        try:
            # Remove existing container with the same name if it exists
            try:
                existing_container = await asyncio.to_thread(self.docker_client.containers.get, container_name)
                await asyncio.to_thread(existing_container.remove, force=True)
                self.log.info(f"Removed existing container with name: {container_name}")
            except Exception:
                pass  # Container does not exist, continue

            container = await asyncio.to_thread(
                self.docker_client.containers.create,
                image_tag,
                detach=True,
                network=self.network_name,
                name=container_name,
                volumes={
                    os.path.abspath(self.host_data_path): {"bind": "/app/data", "mode": "ro"},
                    os.path.abspath(self.host_output_path): {"bind": "/app/output", "mode": "rw"},
                },
                labels={"app": "dataops-assistant", "pipeline_id": pipeline_id},
            )
            self.log.info(f"Docker container created for pipeline ID: {pipeline_id}, Container ID: {container.id}")
            # Remove the container immediately after creation (test only)
            await asyncio.to_thread(container.remove, force=True)
            self.log.info(f"Docker container removed for pipeline ID: {pipeline_id}, Container ID: {container.id}")
            return {"success": True, "image_id": image.id}
        except Exception as e:
            self.log.error(f"Failed to create/remove Docker container: {e}")
            return {"success": False, "details": f"Failed to create/remove Docker container: {e}"}


    async def run_pipeline_in_container(self, image_id: str) -> dict:
        """
        Given an image ID, create and run a new container, wait for it to finish, and return the logs and exit status.
        """
        try:
            container = await asyncio.to_thread(
                self.docker_client.containers.run,
                image_id,
                detach=True,
                network=self.network_name,
                    volumes={
                        os.path.abspath(self.host_data_path): {"bind": "/app/data", "mode": "ro"},
                        os.path.abspath(self.host_output_path): {"bind": "/app/output", "mode": "rw"},
                    },
                labels={"app": "dataops-assistant"},
            )
            self.log.info(f"Started container from image {image_id}, Container ID: {container.id}")
            await asyncio.to_thread(container.wait)
            logs = await asyncio.to_thread(container.logs)
            logs = logs.decode('utf-8')
            exit_code = container.attrs['State']['ExitCode']
            self.log.info(f"Container logs:\n{logs}")
            await asyncio.to_thread(container.remove, force=True)
            if exit_code == 0:
                return {"success": True, "details": "Pipeline ran successfully", "logs": logs}
            else:
                return {"success": False, "details": "Pipeline failed", "logs": logs}
        except Exception as e:
            self.log.error(f"Failed to run pipeline in container: {e}")
            return {"success": False, "details": f"Failed to run pipeline in container: {e}"}