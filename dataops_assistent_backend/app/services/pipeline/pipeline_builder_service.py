import logging
import jsonschema


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
import pandas as pd
import datetime

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
        # Add other initializations as needed

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


    async def build_pipeline_with_templates(self, user_input: str, output_dir: str = "pipelines") -> dict:
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
            db_info = await self.source_service.connect_to_source(spec)
            if not db_info.get("success"):
                self.log.error("Source/Destination connection failed.")
                return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}
            
            
            # Step 5: Generate pipeline code 
            self.log.info("Generating pipeline code...")
            pipeline_code = await self.code_gen.generate_code(spec, db_info)
            
            # Step 8: Create pipeline files in MinIO (instead of local files)
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
            
            # Step 9: Run tests from MinIO storage
            self.log.info("Running pipeline tests from MinIO storage...")
            try:
                test_result = await self.test_service.run_pipeline_test_in_venv(
                    pipeline_id,  # Pass pipeline_id instead of folder path
                )
                self.log.info(f"Test result: {test_result}")
            except Exception as e:
                self.log.error(f"Failed to run pipeline tests: {e}")
                test_result = {"success": False, "details": f"Test execution failed: {e}"}
            
            execution_time = (datetime.datetime.now() - start_time).seconds
            message = f"Template-based pipeline created successfully in {execution_time} seconds"
            
            self.log.info(message)
            
            return {
                "success": True,
                "pipeline_id": pipeline_id,  # Add pipeline_id to response
                "spec": spec,
                # "requirements": requirements,
                # "storage_info": pipeline_info,  # MinIO storage information
                # "test_result": test_result,    # Test execution results
                # "folder": pipeline_info.get("folder"),  # Virtual folder path for compatibility
                # "message": message,
                # "execution_time": execution_time,
                # "mode": "template-based-minio"
            }
            
        except Exception as e:
            self.log.error(f"Template-based pipeline creation failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to create pipeline: {e}"
            }
