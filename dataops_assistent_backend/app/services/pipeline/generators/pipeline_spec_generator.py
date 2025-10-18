import json
from app.services.llm_service import LLMService
import datetime


ETL_SPEC_SCHEMA = {
    "type": "object",
    "properties": {
        "pipeline_name": {
            "type": "string",
            "description": "A name for the pipeline in snake_case format",
        },
        "source_type": {
            "type": "string",
            "description": "The source of the data",
            "enum": ["localFileCSV", "localFileJSON", "PostgreSQL", "api"],
        },
        "source_table": {
            "type": "string",
            "description": "The name of the source table (if source is a database)",
        },
        "source_path": {
            "type": "string",
            "description": "The path to the source file or API endpoint",
        },
        "destination_type": {
            "type": "string",
            "description": "The destination for the data",
            "enum": ["PostgreSQL", "sqlite", "parquet"],
        },
        "destination_name": {
            "type": "string",
            "description": "The name of the destination table or file without extension",
        },
        "transformation_logic": {
            "type": "string",
            "description": "Specific transformation logic to apply to the data (e.g., 'filter rows where age > 25', 'add calculated column revenue = price * quantity')",
        },
        "schedule": {
            "type": "string",
            "description": "Cron schedule for the pipeline",
        }
    },
    "required": ["pipeline_name", "source_type", "source_path", "source_table", "destination_type", "destination_name", "transformation_logic", "schedule"],
    "additionalProperties": False,
}
class PipelineSpecGenerator:
    """
    Service for generating pipeline specifications (specs) for ML/data pipelines.
    """
    def __init__(self):
        self.llm = LLMService()

    async def generate_spec(self, user_input: str) -> dict:
        """
        Generate a pipeline specification from user input.
        Now focuses on configuration extraction rather than full code generation.
        Args:
            user_input (str): Description or requirements for the pipeline.
        Returns:
            dict: A dictionary representing the pipeline specification.
        """
        try:
            prompt = f"""
            Extract pipeline configuration from this request: {user_input}
            
            Focus on identifying:
            - Data source type and location (file path, table name, API endpoint)
            - Data destination type and name (table name, file name)
            - Any specific transformation requirements
            - Schedule requirements
            
            For transformation_logic, extract only the specific business logic needed (e.g., 'filter active users', 'calculate monthly totals').
            If no transformation is specified, leave it empty.
            
            For required fields that aren't specified:
            - source_path is REQUIRED for localFileCSV, localFileJSON, and api sources
            - source_table is REQUIRED for PostgreSQL sources  
            - Set schedule to '0 0 * * *' (daily at midnight) if not specified
            - For file sources without explicit paths, suggest reasonable defaults like 'data/input.csv'
            """
            
            response = await self.llm.response_create_async(
                input = prompt,
                text={
                "format": {
                    "type": "json_schema",
                    "name": "extract_json",
                    "strict": True,
                    "schema": ETL_SPEC_SCHEMA,
                    }
                }
            )
        except Exception as e:
            raise RuntimeError(f"LLM request failed: {e}")

        print(response)  # For debugging purposes

        spec = json.loads(response.output_text)

        print(f"Generated spec: {spec}")  # For debugging purposes

        # Validate required fields based on source type
        self._validate_spec_requirements(spec)

        # Add timestamp to pipeline name
        date_str = datetime.datetime.now().strftime('%Y%m%d_%H%M')
        if 'pipeline_name' in spec:
            spec['pipeline_name'] = f"{spec['pipeline_name']}_{date_str}"
            
        return spec

    def _validate_spec_requirements(self, spec: dict) -> None:
        """Validate that required fields are present based on source type."""
        source_type = spec.get('source_type')
        
        # Check source-specific requirements
        if source_type in ['localFileCSV', 'localFileJSON', 'api']:
            if not spec.get('source_path'):
                raise ValueError(f"source_path is required for source_type '{source_type}'")
        elif source_type == 'PostgreSQL':
            if not spec.get('source_table'):
                raise ValueError(f"source_table is required for source_type '{source_type}'")
        
        # Ensure transformation_logic exists (can be empty string)
        if 'transformation_logic' not in spec:
            spec['transformation_logic'] = ""
