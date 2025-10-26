import logging
import jsonschema
import pandas as pd
import datetime


from .guards.prompt_guard_service import PromptGuardService
from app.services.llm_service import LLMService
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
        self.guard = PromptGuardService()
        self.llm = LLMService()
        self.spec_gen = PipelineSpecGenerator()
        self.local_file_service = LocalFileService()
        self.database_service = get_database_service() 
        self.code_gen = PipelineCodeGeneratorLLMHybrid(self.log)
        self.output_service = PipelineOutputService()
        self.test_service = PipelineTestService(self.log)
        self.source_service = SourceService(self.log)
        self.dockerize_service = DockerizeService(self.log)
        self.scheduler_service = SchedulerService(self.log)


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
                if not spec.get("source_path", "").endswith('.json'):
                    return False
            case _:
                pass
        return True


    async def build_pipeline(self, user_input: str, output_dir: str = "pipelines") -> dict:
        """
        Build a pipeline using the new template-based approach.
        This is a more efficient alternative to the full build_pipeline method.
        """
        try:
            start_time = datetime.datetime.now()
            
            # Step 1: Generate pipeline specification
            self.log.info("Generating pipeline specification...")
            spec = await self.spec_gen.generate_spec(user_input)
            
            # Step 2: Validate schema
            self.log.info("Validating pipeline specification schema...")
            if not self.validate_spec_schema(spec):
                self.log.error("Pipeline specification schema validation failed.")
                return {"error": "Spec schema validation failed."}
            
            # step 3. Try connecting to source/destination
            self.log.info("Connecting to source/destination to validate access...")
            db_info = await self.source_service.fetch_data_from_source(spec, limit=5)
            if not db_info.get("success"):
                self.log.error("Source/Destination connection failed.")
                return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}
            
            # Step 4: Generate pipeline code 
            self.log.info("Generating pipeline code...")
            pipeline_code = await self.code_gen.generate_code(spec, db_info)
            
            # Step 5: Create pipeline files in MinIO (instead of local files)
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

            # Step 7: Iterate to perfect the pipeline based on test results (if needed)

            # Step 8: Dockerize and deploy the pipeline
            self.log.info("Dockerizing and deploying the pipeline...")
            dockerize_result = await self.dockerize_service.dockerize_pipeline(pipeline_id)
            if not dockerize_result.get("success"):
                self.log.error("Dockerization failed.", dockerize_result)
                return {
                    "success": False,
                    "error": "Dockerization failed.",
                }


            # Step 9: Save pipeline to catalog.json for Airflow scheduling
            scheduled_result = await self.scheduler_service.save_pipeline_to_catalog(
                pipeline_id,
                spec
            )

            if not scheduled_result.get("success"):
                self.log.error("Scheduling failed.", scheduled_result)
                return {
                    "success": False,
                    "error": "Scheduling failed.",
                }
            # TODO: Step 9: e2e testing


            execution_time = (datetime.datetime.now() - start_time).seconds
            message = f"Template-based pipeline created successfully in {execution_time} seconds"
            
            response = {
                "success": True,
                "pipeline_id": pipeline_id,  # Add pipeline_id to response
                "spec": spec,
                "test_result": test_result,    # Test execution results
                "message": message,
                "dockerize_result": dockerize_result,
                "scheduling_result": scheduled_result
            }
            
            self.log.info(f"Pipeline creation successful: {response}")

            return response

        except Exception as e:
            self.log.error(f"Template-based pipeline creation failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to create pipeline: {e}"
            }
