import json
import logging
import jsonschema
import datetime
from yaspin import yaspin
from shared.utils.spinner_utils import run_step_with_spinner
import logging

from shared.services.llm_service import LLMService
from pipeline_builder.registry.pipeline_registry_service import getPipelineRegistryService
from shared.models.pipeline_types import PipelineBuildResponse
from .generators.pipeline_spec_generator import PipelineSpecGenerator
from .generators.pipeline_code_generator_LLM_hybrid import PipelineCodeGeneratorLLMHybrid
from .generators.pipeline_spec_generator import ETL_SPEC_SCHEMA
from .sources.local_file_service import LocalFileService
from .testing.pipeline_test_service import PipelineTestService
from shared.services.database_service import get_database_service
from .deployment.pipeline_output_service import PipelineOutputService
from .sources.source_service import SourceService
from .deployment.dockerize_service import DockerizeService
from .deployment.scheduler_service import SchedulerService

class PipelineBuilderService:
    def __init__(self):
        self.log = logging.getLogger("dataops")
        self.llm = LLMService()
        self.spec_gen = PipelineSpecGenerator(self.log)
        self.local_file_service = LocalFileService(self.log)
        self.database_service = get_database_service() 
        self.code_gen = PipelineCodeGeneratorLLMHybrid(self.log)
        self.output_service = PipelineOutputService()
        self.test_service = PipelineTestService(self.log)
        self.source_service = SourceService(self.log)
        self.dockerize_service = DockerizeService(self.log)
        self.scheduler_service = SchedulerService(self.log)
        self.pipeline_registry = getPipelineRegistryService()


    async def validate_spec_schema(self, spec: dict) -> bool:
        # Validate spec against ETL_SPEC_SCHEMA using jsonschema
        try:
            jsonschema.validate(instance=spec, schema=ETL_SPEC_SCHEMA)
            return self.validate_source_path(spec)
        except ImportError:
            print("jsonschema package is not installed.")
            return False
        except jsonschema.ValidationError as e:
            print(f"Schema validation error: {e}")
            return False


    def validate_source_path(self, spec: dict) -> None:
        # If source_type is localFileCSV or localFileJSON, ensure source_path exists
        match spec.get("source_type"):
            case "localFileCSV":
                if not spec.get("source_path", "").endswith('.csv'):
                    return False
            case "localFileJSON":
                if not spec.get("source_path", "").endswith('.jsonl'):
                    return False
            case _:
                pass
        return True


    async def build_pipeline(self, user_input: str, fast: bool = False, mode: str = "chat", run_after_deploy: bool = False) -> PipelineBuildResponse:
        """
        Build a pipeline using the new template-based approach.
        This is a more efficient alternative to the full build_pipeline method.
        """
        try:

            start_time = datetime.datetime.now()
            # Step 1: Generate pipeline specification
            build_step = "generate_spec"
            step_msg = "Generating pipeline specification..."
            self.log.info(f"[STEP: {build_step}] {step_msg}")
            step_number = 1
            spec, error = await self._run_step(step_msg, step_number, self.spec_gen.generate_spec, user_input, mode=mode)
            if error:
                self.log.error(f"Failed to generate pipeline specification: {error}")
                return {"error": f"Failed to generate pipeline specification: {error}"}
            
            # Step 2: Validate schema
            build_step = "validate_spec"
            step_msg = "Validating pipeline specification schema..."
            step_number = 2
            self.log.info(f"[STEP: {build_step}] {step_msg}")
         
            isValidSpec, error = await self._run_step(step_msg, step_number, self.validate_spec_schema, spec, mode=mode)
            if not isValidSpec or error:
                self.log.error("Pipeline specification schema validation failed.")
                return {"error": "Spec schema validation failed.", "spec": spec}
            
            # Step 3: Try connecting to source/destination
            build_step = "validate_source_connection"
            step_msg = "Connecting to source/destination to validate access..."
            step_number = 3
            self.log.info(f"[STEP: {build_step}] {step_msg}")
          
            db_info, error  = await self._run_step(step_msg, step_number, self.source_service.fetch_data_from_source, spec, limit=5, mode=mode)
            if error or not db_info.get("success"):
                self.log.error("Source/Destination connection failed.")
                return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}
            
            # Step 4: Generate pipeline code 
            build_step = "generate_code"
            step_msg = "Generating pipeline code..."
            step_number = 4
            self.log.info(f"[STEP: {build_step}] {step_msg}")
    

            pipeline_code, error = await self._run_step(step_msg, step_number, self.code_gen.generate_code, spec, db_info, mode=mode)
            if error:
                self.log.error(f"Failed to generate pipeline code: {error}")
                return {"error": f"Failed to generate pipeline code: {error}"}
            
            # Step 5: Store pipeline files
            
            build_step = "store_pipeline_files"
            step_msg = "Storing pipeline files in MinIO..."
            step_number = 5

            self.log.info(f"[STEP: {build_step}] {step_msg}")
    
            try:
                pipeline_info, error = await self._run_step(step_msg, step_number, self.output_service.store_pipeline_files,
                    spec.get("pipeline_name"), 
                    pipeline_code, 
                    mode=mode
                )
                if error:
                    self.log.error(f"Failed to store pipeline files in MinIO: {error}")
                    return {"error": f"Failed to store pipeline files: {error}"}
                
                pipeline_id = pipeline_info["pipeline_id"]
                self.log.info(f"Pipeline files stored in MinIO with ID: {pipeline_id}")
            except Exception as e:
                self.log.error(f"Failed to store pipeline files in MinIO: {e}")
                return {"error": f"Failed to store pipeline files: {e}"}
            
            # Step 6: Run tests 
            if not fast:
                build_step = "run_pipeline_tests"
                step_msg = "Running pipeline tests..."
                step_number = 6
                self.log.info(f"[STEP: {build_step}] {step_msg}")
                try:
                    test_result, error = await self._run_step(step_msg, step_number, self.test_service.run_pipeline_test_in_venv_v2,
                        pipeline_id,  # Pass pipeline_id instead of folder path
                        mode=mode
                    )
                    self.log.info(f"Test result: {test_result}")
                    if error or not test_result.get("success"):
                        self.log.error("Pipeline tests failed.", test_result)
                        return {
                            "success": False,
                            "error": "Pipeline tests failed.",
                        }
                except Exception as e:
                    self.log.error(f"Failed to run pipeline tests: {e}")
                    test_result = {"success": False, "details": f"Test execution failed: {e}"}

                if not test_result.get("success"):
                    self.log.error("Pipeline tests failed.")
                    return {
                        "pipeline_name": spec.get("pipeline_name"),
                        "pipeline_id": pipeline_id,
                        "success": False,
                        "build_steps_completed": build_step,
                        "error": "Pipeline tests failed.",
                        "test_result": test_result
                    }
            else:
                if mode == "cmd":
                    print("\033[93m[Step 6]: Run Pipeline Tests Fast mode enabled; skipping tests.\033[0m")

                self.log.info("Fast mode enabled; skipping tests.")

                test_result = {"skipped": True, "details": "Skipped tests in fast mode."}

            # Step 7: Register pipeline in the registry if tests passed
            if test_result.get("success") or test_result.get("skipped"):
                build_step = "register_pipeline"
                step_msg = "Registering pipeline in the registry..."
                self.log.info(f"[STEP: {build_step}] {step_msg}")
                step_number = 7
                try:
                    _, error = await self._run_step(step_msg, step_number, self.pipeline_registry.create_pipeline,
                    pipeline_id=pipeline_id,
                    name=spec.get("pipeline_name"),
                    created_by=spec.get("created_by", "unknown"),
                    description=spec.get("description", ""),
                    spec=spec,
                    mode=mode
                    )
                    if error:
                        self.log.error(f"Failed to register pipeline: {error}")
                        return {"success": False, "details": f"Failed to register pipeline: {error}"}
                    
                    self.log.info(f"Pipeline {pipeline_id} registered successfully.")
                except Exception as e:
                    self.log.error(f"Failed to register pipeline: {e}")
                    return {"success": False, "details": f"Failed to register pipeline: {e}"}
                
            # TODO: Step 7: Iterate to perfect the pipeline based on test results (if needed)

            # Step 8: Test code in docker container - test runner
            build_step = "Test_pipeline_in_docker"
            step_msg = "Testing the pipeline in Docker container..."
            step_number = 8
            self.log.info(f"[STEP: {build_step}] {step_msg}")
     
            try:
                test_runner_result, error = await self._run_step(step_msg, step_number, self.dockerize_service.test_pipeline_in_docker, pipeline_id, mode=mode)
                self.log.info(f"Test runner result:\n{json.dumps(test_runner_result, indent=2)}")
                if error:
                    self.log.error(f"Test runner failed: {error}")
                    return {"success": False, "details": f"Failed to test the pipeline in Docker container: {error}"}
            except Exception as e:
                self.log.error(f"Failed to test the pipeline in Docker container: {e}")
                return {"success": False, "details": f"Failed to test the pipeline in Docker container: {e}"}
            if not test_runner_result.get("success"):
                self.log.error("Dockerization failed.")
                return {
                    "pipeline_name": spec.get("pipeline_name"),
                    "build_steps_completed": build_step,
                    "pipeline_id": pipeline_id,
                    "success": False,
                    "error": "Docker test in test runner failed.",
                    "test_runner_result": test_runner_result
                }
            
            # Step 9: Dockerizeition 
            build_step = "Dockerizeition"
            step_msg = "Dockerizing and deploying the pipeline..."
            step_number = 9
            self.log.info(f"[STEP: {build_step}] {step_msg}")
            try:
                dockerize_result, error = await self._run_step(step_msg, step_number, self.dockerize_service.dockerize_pipeline_v2,
                    pipeline_id,
                )
                self.log.info(f"Dockerizeition result:\n{json.dumps(dockerize_result, indent=2)}")
                if error:
                    self.log.error(f"Dockerizeition failed: {error}")
                    return {"success": False, "details": f"Failed to Dockerize the pipeline: {error}"}
            except Exception as e:
                self.log.error(f"Failed to Dockerize the pipeline: {e}")
                return {"success": False, "details": f"Failed to Dockerize the pipeline: {e}"}
            
            # Step 10: Scheduling in airflow
            build_step = "schedule_pipeline"
            step_msg = "Scheduling the pipeline..."
            step_number = 10
            self.log.info(f"[STEP: {build_step}] {step_msg}")

            if spec.get("schedule") and spec.get("schedule") != "manual":
                scheduled_result, error = await self._run_step(step_msg, step_number, self.scheduler_service.save_pipeline_to_catalog,
                    pipeline_id,
                    spec
                )
                if error:
                    self.log.error(f"Failed to schedule the pipeline: {error}")
                    return {"success": False, "details": f"Failed to schedule the pipeline: {error}"}
            else:
                scheduled_result = {"success": True, "details": "Pipeline set to manual schedule; not added to catalog."}

            await self.pipeline_registry.update_pipeline(
                pipeline_id,
                {
                    "status": "deployed"
                }
            )

            if run_after_deploy:
                step_msg = "Running the pipeline once after deployment..."
                step_number = 11
                # Run the pipeline once after deployment
                self.log.info("Running the pipeline once after deployment...")
                run_result, error = await self._run_step( step_msg, step_number,
                    self.dockerize_service.run_pipeline_in_container, dockerize_result.get("container_id"))
                if error:
                    self.log.error(f"Failed to run the pipeline after deployment: {error}")
                else:
                    self.log.info(f"Pipeline run result after deployment:\n{json.dumps(run_result, indent=2)}")

            execution_time = (datetime.datetime.now() - start_time).seconds
            message = f"Pipeline created successfully in {execution_time} seconds"
            
            self.log.info(message)
            if mode == "cmd":
                print(f"\n\033[94mDone! {message}\033[0m")

            response = {
                "pipeline_name": spec.get("pipeline_name"),
                "pipeline_id": pipeline_id, 
                "container_id": dockerize_result.get("container_id"),
                "dockerize_result": dockerize_result,
                "build_steps_completed": build_step,
                "success": True,
                "request_spec": spec,
                "test_result": test_result,
                "message": message,
                "test_runner_result": test_runner_result ,
                "scheduling_result": scheduled_result,
                "execution_time": execution_time
            }

            if run_after_deploy:
                response["run_result_after_deploy"] = run_result

            self.log.info("Pipeline build response:\n%s", json.dumps(response, indent=2))
            
            return response
            
        except Exception as e:
            self.log.error(f"Failed to create pipeline: {e}")
            return {
                "success": False,
                "pipeline_name": spec.get("pipeline_name"),
                "pipeline_id": pipeline_id if 'pipeline_id' in locals() else None,
                "build_steps_completed": build_step,
                "error": str(e),
                "message": f"Failed to create pipeline: {e}"
            }

    async def _run_step(self, step_msg: str, step_number: int, coro, *args, mode="chat", **kwargs):
        """
        Helper to run an async step with optional CLI spinner.
        Returns (result, error). If error is not None, result is None.
        """
        if mode == "cmd":
            return await run_step_with_spinner(step_msg, step_number, coro, *args, **kwargs)
        else:
            try:
                result = await coro(*args, **kwargs)
                return result, None
            except Exception as e:
                return None, e
