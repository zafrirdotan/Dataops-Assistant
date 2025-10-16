import logging
import jsonschema


from .guards.prompt_guard_service import PromptGuardService
from app.services.llm_service import LLMService
from .generators.pipeline_spec_generator import PipelineSpecGenerator
from .generators.pipeline_code_generator_LLM_manual import PipelineCodeGeneratorLLMManual
from .generators.pipeline_spec_generator import ETL_SPEC_SCHEMA
from .sources.local_file_service import LocalFileService
from .testing.pipeline_test_service import PipelineTestService
from app.services.database_service import get_database_service
from app.utils.json_utils import make_json_serializable
from .deployment.pipeline_output_service import PipelineOutputService
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
        self.code_gen = PipelineCodeGeneratorLLMManual(self.log)
        self.output_service = PipelineOutputService()
        self.test_service = PipelineTestService(self.log)
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

    async def connect_to_source(self, spec: dict) -> dict:
        # Try connecting to source/destination based on spec

        match spec.get("source_type"):
            case "PostgreSQL":
                try: 
                    local_db = await self.database_service.test_connection()
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
                            data = await self.database_service.fetch_all(f"SELECT * FROM {table_name} LIMIT 5")
                            self.log.info(f"PostgreSQL data fetch result: {data}")
                            if data is not None and len(data) > 0:
                                # Get column names
                                table_only = source_table.split('.')[-1]  # Extract table name without schema
                                schema_name = table_name.split('.')[0] if '.' in table_name else 'public'
                                columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_only}' AND table_schema = '{schema_name}' ORDER BY ordinal_position"
                                column_results = await self.database_service.fetch_all(columns_query)
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
                    data = await self.local_file_service.retrieve_recent_data_files(spec.get("source_path"), date_column="event_date", date_value="2025-09-18")
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
                if await self.local_file_service.check_file_exists(spec.get("source_path")):
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
            db_info = await self.connect_to_source(spec)
            if not db_info.get("success"):
                self.log.error("Source/Destination connection failed.")
                return {"error": "Source/Destination connection failed.", "details": db_info.get("details")}
            
            # Step 4: Convert data preview to DataFrame if available
            data_preview_df = None
            if db_info.get("data_preview"):
                try:
                    data_preview_df = pd.DataFrame(db_info.get("data_preview"))
                    self.log.info(f"Converted data preview to DataFrame with shape: {data_preview_df.shape}")
                except Exception as e:
                    self.log.warning(f"Could not convert data preview to DataFrame: {e}")
            
            # Step 5: Generate pipeline code 
            self.log.info("Generating pipeline code...")
            pipeline_code = await self.code_gen.generate_code(spec, data_preview_df)
            
            # Step 6: Generate test code
            self.log.info("Generating test code...")
            test_code = await self.code_gen.generate_test_code(spec, data_preview_df)
            
            # Step 7: Generate requirements.txt
            requirements = self.code_gen.generate_requirements_txt()
            
            # Step 8: Create pipeline files in MinIO (instead of local files)
            try:
                pipeline_info = await self.output_service.create_pipeline_files(
                    spec.get("pipeline_name"), 
                    pipeline_code, 
                    requirements, 
                    test_code
                )
                pipeline_id = pipeline_info["pipeline_id"]
                self.log.info(f"Pipeline files stored in MinIO with ID: {pipeline_id}")
            except Exception as e:
                self.log.error(f"Failed to store pipeline files in MinIO: {e}")
                return {"error": f"Failed to store pipeline files: {e}"}
            
            # Step 9: Run tests from MinIO storage
            self.log.info("Running pipeline tests from MinIO storage...")
            try:
                test_result = await self.test_service.run_pipeline_test(
                    pipeline_id,  # Pass pipeline_id instead of folder path
                    spec.get("pipeline_name"), 
                    execution_mode="venv"
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
                "code": pipeline_code,
                "test_code": test_code,
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
