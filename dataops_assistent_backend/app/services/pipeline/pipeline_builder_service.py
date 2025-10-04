import logging
import jsonschema
import runpy

from .generators.pipeline_code_generator import PipelineCodeGenerator
from .guards.prompt_guard_service import PromptGuardService
from app.services.llm_service import LLMService
from .generators.pipeline_spec_generator import PipelineSpecGenerator
from .generators.pipeline_spec_generator import ETL_SPEC_SCHEMA
from .sources.local_file_service import LocalFileService
from .testing.pipeline_test_service import PipelineTestService
from app.services.database_service import get_database_service
from app.utils.json_utils import make_json_serializable
from .deployment.pipeline_output_service import PipelineOutputService
import pandas as pd

class PipelineBuilderService:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.guard = PromptGuardService()
        self.llm = LLMService()
        self.spec_gen = PipelineSpecGenerator()
        self.local_file_service = LocalFileService()
        self.database_service = get_database_service() 
        self.code_gen = PipelineCodeGenerator()
        self.output_service = PipelineOutputService()
        self.test_service = PipelineTestService(self.log)
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

        # 5-7. Generate pipeline code, build files, and run unit test, retry if unit test fails
        generate_attempts = 0
        code = None
        python_test = None
        last_error = None
        while True:
            generate_attempts += 1

            self.log.info("Generating pipeline code...")
            try:
                code, requirements, python_test = self.code_gen.generate_code(spec,
                                                                            db_info.get("data_preview"),
                                                                            last_code=code,
                                                                            last_error=last_error,
                                                                            python_test=python_test
                                                                            )
            except Exception as e:
                self.log.error(f"Pipeline code generation failed: {e}")
                return {"error": f"Pipeline code generation failed: {e}"}
            
            if not code:
                self.log.error("Pipeline code generation failed.")
                return {"error": "Pipeline code generation failed."}

            self.log.info("Creating and running unit tests...")

            try:
                folder = self.output_service.create_pipeline_files(spec.get("pipeline_name"), code, requirements, python_test)
            except Exception as e:
                self.log.error(f"Failed to create pipeline files: {e}")

            self.log.info("Running pipeline tests...")

            try:
                test_result = self.test_service.run_pipeline_test(folder, spec.get("pipeline_name"), execution_mode="venv")
            except Exception as e:
                self.log.error(f"Failed to run pipeline tests: {e}")


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

        # # 8. Deploy
        # deploy_result = self.deploy_pipeline(code)
        # if not deploy_result.get("success"):
        #     return {"error": "Deployment failed.", "details": deploy_result.get("details")}

        # # 9. E2E tests
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
                    local_db = self.database_service.test_connection()
                    self.log.info(f"PostgreSQL connection test result: {local_db}")

                    data_preview = []
                    if local_db:
                        source_table = spec.get('source_table')
                        self.log.info(f"source_table from spec: {source_table}")
                        if not source_table:
                            return {"success": False, "error": "source_table is required for PostgreSQL source"}
                        
                        # Handle table name format (with or without schema)
                        if '.' not in source_table:
                            # If no schema specified, assume public schema
                            table_name = f"public.{source_table}"
                        else:
                            table_name = source_table
                        
                        self.log.info(f"Fetching data from table: {table_name}")
                        try:
                            data = self.database_service.fetch_all(f"SELECT * FROM {table_name} LIMIT 5")
                            self.log.info(f"PostgreSQL data fetch result: {data}")
                            if data is not None and len(data) > 0:
                                # Get column names
                                table_only = source_table.split('.')[-1]  # Extract table name without schema
                                schema_name = table_name.split('.')[0] if '.' in table_name else 'public'
                                columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_only}' AND table_schema = '{schema_name}' ORDER BY ordinal_position"
                                column_results = self.database_service.fetch_all(columns_query)
                                columns = [row[0] for row in column_results] if column_results else None
                                
                                # Convert to DataFrame for easier handling
                                df = pd.DataFrame(data, columns=columns if columns else None)
                                raw_preview = df.head().to_dict(orient="records")
                                # Make JSON serializable
                                data_preview = make_json_serializable(raw_preview)
                                self.log.info(f"PostgreSQL data preview: {data_preview}")
                            else:
                                self.log.warning(f"No data found in table {table_name}")
                        except Exception as e:
                            self.log.error(f"Error fetching data from {table_name}: {e}")
                            return {"success": False, "details": f"Error fetching data from table {table_name}: {e}"}
                        
                        return {"success": True, "data_preview": data_preview}
                    else:
                        return {"success": False, "details": "Could not connect to PostgreSQL database"}
                except Exception as e:
                    self.log.error(f"PostgreSQL connection error: {e}")
                    return {"failed": False, "details": "Failed to connect to PostgreSQL source."}

            case "localFileCSV":
                try:
                    data = self.local_file_service.retrieve_recent_data_files(spec.get("source_path"), date_column="event_date", date_value="2025-09-18")
                    if data is not None:
                        raw_preview = data.head().to_dict(orient="records")
                        # Make JSON serializable
                        data_preview = make_json_serializable(raw_preview)
                        return {"success": True, "data_preview": data_preview}
                    else:
                        return {"success": False, "details": "No recent data files found."}
                except Exception as e:
                    return {"failed": False, "details": "Failed to connect to local CSV source."}
            case "localFileJSON":
                if self.local_file_service.check_file_exists(spec.get("source_path")):
                    # TODO: Implement JSON file reading and data preview generation
                    data_preview = []  # Placeholder until JSON reading is implemented
                    return {"success": True, "data_preview": data_preview}
                else:
                    return {"success": False, "details": "No recent data files found."}
            case "sqlLite":
                pass
            case "api":
                pass

        return {"success": True}

    def deploy_pipeline(self, code: str) -> dict:
        # TODO: Implement deployment logic
        return {"success": True}

    def run_e2e_tests(self, deploy_result: dict) -> dict:
        # TODO: Implement E2E test logic
        return {"success": True}
