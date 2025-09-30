import logging
import jsonschema
import runpy

from app.services.generators.pipeline_code_generator import PipelineCodeGenerator
from app.services.guards.prompt_guard_service import PromptGuardService
from app.services.llm_service import LLMService
from app.services.generators.pipeline_spec_generator import PipelineSpecGenerator
from app.services.generators.pipeline_spec_generator import ETL_SPEC_SCHEMA
from app.services.source.local_file_service import LocalFileService
from app.services.tests.test_pipline_service import TestPipelineService

class PipelineBuilderService:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.guard = PromptGuardService()
        self.llm = LLMService()
        self.spec_gen = PipelineSpecGenerator()
        self.local_file_service = LocalFileService()
        self.code_gen = PipelineCodeGenerator()
        self.test_service = TestPipelineService(self.log)
        # Add other initializations as needed

    def build_pipeline(self, user_input: str) -> dict:
        # 2. Generate JSON spec
        self.log.info("Generating pipeline specification...")
        spec = self.spec_gen.generate_spec(user_input)

        # 3. Validate schema
        self.log.info("Validating pipeline specification schema...")
        if not self.validate_spec_schema(spec):
            self.log.error("Pipeline specification schema validation failed.")
            return {"error": "Spec schema validation failed."}

        # 4. Try connecting to source/destination
        self.log.info("Connecting to source/destination to validate access...")
        db_info = self.connect_to_source(spec)
        if not db_info.get("success"):
            self.log.error("Source/Destination connection failed.")
            return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}

        # 5-6. Generate pipeline code and run unit test, retry if unit test fails
        generate_attempts = 0
        code = None
        python_test = None
        last_error = None
        while True:
            generate_attempts += 1

            self.log.info("Generating pipeline code...")
            code, requirements, python_test = self.code_gen.generate_code(spec,
                                                                            db_info.get("data_preview"),
                                                                            last_code=code,
                                                                            last_error=last_error,
                                                                            python_test=python_test
                                                                            )
            if not code:
                self.log.error("Pipeline code generation failed.")
                return {"error": "Pipeline code generation failed."}

            self.log.info("Creating and running unit tests...")
            test_result = self.create_and_run_unittest(spec, code, requirements, python_test)
            if test_result.get("success"):
                break
            else:
                last_error = test_result.get("details")
                self.log.error("Unit test failed. Retrying pipeline code generation...")
            # Optionally, add a retry limit to avoid infinite loops
            if generate_attempts > 3:
                self.log.error("Max retry attempts reached.")
                return {"error": "Max retry attempts reached."}
        self.log.info("Pipeline code generation and unit tests completed successfully. After %d attempts.", generate_attempts)

        # # 7. Deploy
        # deploy_result = self.deploy_pipeline(code)
        # if not deploy_result.get("success"):
        #     return {"error": "Deployment failed.", "details": deploy_result.get("details")}

        # # 8. E2E tests
        # e2e_result = self.run_e2e_tests(deploy_result)
        # if not e2e_result.get("success"):
        #     return {"error": "E2E tests failed.", "details": e2e_result.get("details")}

        return {
            "success": True,
            "spec": spec,
            "code": code,
            # "unit_test": test_result,
            # "deployment": deploy_result,
            # "e2e_test": e2e_result
        }

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

    def connect_to_source(self, spec: dict) -> dict:
        # Try connecting to source/destination based on spec

        match spec.get("source_type"):
            case "PostgreSQL":
                try: 
                    local_db = self.postgres_service.connect(spec.get("source_config"))
                    if local_db:
                        data = self.postgres_service.retrieve_data(local_db, spec.get("source_table"))
                        if data is not None:
                            data_preview = data.head().to_dict(orient="records")
                        return {"success": True, "data_preview": data_preview}
                    else:
                        return {"failed": False, "details": "No data retrieved from source table."}
                except Exception as e:
                    return {"failed": False, "details": "Failed to connect to PostgreSQL source."}

            case "localFileCSV":
                try:
                    data = self.local_file_service.retrieve_recent_data_files(spec.get("source_path"), date_column="event_date", date_value="2025-09-18")
                    if data is not None:
                        data_preview = data.head().to_dict(orient="records")
                        return {"success": True, "data_preview": data_preview}
                except Exception as e:
                    return {"failed": False, "details": "Failed to connect to local CSV source."}
                else:
                    return {"success": False, "details": "No recent data files found."}
            case "localFileJSON":
                if self.local_file_service.check_file_exists(spec.get("source_path")):
                    return {"success": True, "data_preview": data_preview}
                else:
                    return {"success": False, "details": "No recent data files found."}
            case "sqlLite":
                pass
            case "api":
                pass

        return {"success": True}

    def create_and_run_unittest(self, spec: dict, code: str, requirements: str, python_test: str) -> dict:
        return self.test_service.create_and_run_unittest(spec.get("pipeline_name"), code, requirements, python_test, execution_mode="venv")

    def deploy_pipeline(self, code: str) -> dict:
        # TODO: Implement deployment logic
        return {"success": True}

    def run_e2e_tests(self, deploy_result: dict) -> dict:
        # TODO: Implement E2E test logic
        return {"success": True}
