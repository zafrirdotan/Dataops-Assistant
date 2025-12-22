import json
import logging
import jsonschema
import datetime

from app.services.llm_service import LLMService
from app.services.pipeline.registry.pipeline_registry_service import getPipelineRegistryService
from .generators.pipeline_spec_generator import PipelineSpecGenerator
from .generators.pipeline_code_generator_LLM_hybrid import PipelineCodeGeneratorLLMHybrid
from .generators.pipeline_spec_generator import ETL_SPEC_SCHEMA
from .sources.local_file_service import LocalFileService
from .testing.pipeline_test_service import PipelineTestService
from app.services.database_service import get_database_service
from .deployment.pipeline_output_service import PipelineOutputService
from .sources.source_service import SourceService
from .deployment.dockerize_service import DockerizeService
from .deployment.scheduler_service import SchedulerService
class PipelineBuilderService:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.llm = LLMService()
        self.spec_gen = PipelineSpecGenerator(self.log)
        self.local_file_service = LocalFileService()
        self.database_service = get_database_service() 
        self.code_gen = PipelineCodeGeneratorLLMHybrid(self.log)
        self.output_service = PipelineOutputService()
        self.test_service = PipelineTestService(self.log)
        self.source_service = SourceService(self.log)
        self.dockerize_service = DockerizeService(self.log)
        self.scheduler_service = SchedulerService(self.log)
        self.pipeline_registry = getPipelineRegistryService()


    def validate_spec_schema(self, spec: dict) -> bool:
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


    async def build_pipeline(self, user_input: str, output_dir: str = "pipelines", fast: bool = False) -> dict:
        """
        Build a pipeline using the new template-based approach.
        This is a more efficient alternative to the full build_pipeline method.
        """
        try:
            start_time = datetime.datetime.now()
            # Step 1: Generate pipeline specification
            build_step = "generate_spec"
            self.log.info("Generating pipeline specification...")
            spec = await self.spec_gen.generate_spec(user_input)
            # Step 2: Validate schema
            build_step = "validate_spec"
            self.log.info("Validating pipeline specification schema...")
            if not self.validate_spec_schema(spec):
                self.log.error("Pipeline specification schema validation failed.")
                return {"error": "Spec schema validation failed.", "spec": spec}
            
            # step 3. Try connecting to source/destination
            build_step = "validate_source_connection"
            self.log.info("Connecting to source/destination to validate access...")
            db_info = await self.source_service.fetch_data_from_source(spec, limit=5)
            if not db_info.get("success"):
                self.log.error("Source/Destination connection failed.")
                return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}
            
            # Step 4: Generate pipeline code 
            build_step = "generate_code"
            self.log.info("Generating pipeline code...")
            pipeline_code = await self.code_gen.generate_code(spec, db_info)
            
            # Step 5: Create pipeline files in MinIO (instead of local files)
            build_step = "store_pipeline_files"
            self.log.info("Storing pipeline files in MinIO...")
            try:
                pipeline_info = await self.output_service.store_pipeline_files(
                    spec.get("pipeline_name"), 
                    pipeline_code, 
                )
                pipeline_id = pipeline_info["pipeline_id"]
                self.log.info(f"Pipeline files stored in MinIO with ID: {pipeline_id}")
            except Exception as e:
                self.log.error(f"Failed to store pipeline files in MinIO: {e}")
                return {"error": f"Failed to store pipeline files: {e}"}
            
            # Step 6: Run tests from MinIO storage
            if not fast:
                build_step = "run_pipeline_tests"
                self.log.info("Running pipeline tests from MinIO storage...")
                try:
                    test_result = await self.test_service.run_pipeline_test_in_venv(
                        pipeline_id,  # Pass pipeline_id instead of folder path
                    )
                    self.log.info(f"Test result: {test_result}")
                    if not test_result.get("success"):
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
                test_result = {"skipped": True, "details": "Skipped tests in fast mode."}

            # Register pipeline in the registry if tests passed
            if test_result.get("success") or test_result.get("skipped"):
                self.log.info("Registering pipeline in the registry...")
                build_step = "register_pipeline"
                try:
                    await self.pipeline_registry.create_pipeline(
                    pipeline_id=pipeline_id,
                    name=spec.get("pipeline_name"),
                    created_by=spec.get("created_by", "unknown"),
                    description=spec.get("description", ""),
                    spec=spec
                    )
                    self.log.info(f"Pipeline {pipeline_id} registered successfully.")
                except Exception as e:
                    self.log.error(f"Failed to register pipeline: {e}")
                    return {"success": False, "details": f"Failed to register pipeline: {e}"}
                
            # TODO: Step 7: Iterate to perfect the pipeline based on test results (if needed)

            # Step 8: Dockerize and deploy the pipeline
            build_step = "dockerize_pipeline"
            self.log.info("Dockerizing and deploying the pipeline...")
            try:
                dockerize_result = await self.dockerize_service.build_and_test_docker_image(pipeline_id, spec)
                self.log.info(f"Dockerize result:\n{json.dumps(dockerize_result, indent=2)}")
            except Exception as e:
                self.log.error(f"Failed to dockerize the pipeline: {e}")
                return {"success": False, "details": f"Failed to dockerize the pipeline: {e}"}
            if not dockerize_result.get("success"):
                self.log.error("Dockerization failed.")
                return {
                    "pipeline_name": spec.get("pipeline_name"),
                    "build_steps_completed": build_step,
                    "pipeline_id": pipeline_id,
                    "success": False,
                    "error": "Dockerization failed.",
                    "dockerize_result": dockerize_result
                }
            
            # Step 9: Scheduling in airflow
            build_step = "schedule_pipeline"
            self.log.info("Scheduling the pipeline...")
            if spec.get("schedule") and spec.get("schedule") != "manual":
                scheduled_result = await self.scheduler_service.save_pipeline_to_catalog(
                    pipeline_id,
                    spec
                )
            else:
                scheduled_result = {"success": True, "details": "Pipeline set to manual schedule; not added to catalog."}

            await self.pipeline_registry.update_pipeline(
                pipeline_id,
                {
                    "status": "deployed"
                }
            )

            # Step 9: e2e testing
            execution_time = (datetime.datetime.now() - start_time).seconds
            message = f"Pipeline created successfully in {execution_time} seconds"
            
            self.log.info(message)

            response = {
                "pipeline_name": spec.get("pipeline_name"),
                "pipeline_id": pipeline_id, 
                "build_steps_completed": build_step,
                "success": True,
                "spec": spec,
                "test_result": test_result,
                "message": message,
                "dockerize_result": dockerize_result ,
                "scheduling_result": scheduled_result
            }

            self.log.info("Pipeline build response:\n%s", json.dumps(response, indent=2))
            
            return response
            
        except Exception as e:
            self.log.error(f"Template-based pipeline creation failed: {e}")
            return {
                "success": False,
                "pipeline_name": spec.get("pipeline_name"),
                "pipeline_id": pipeline_id if 'pipeline_id' in locals() else None,
                "build_steps_completed": build_step,
                "error": str(e),
                "message": f"Failed to create pipeline: {e}"
            }
