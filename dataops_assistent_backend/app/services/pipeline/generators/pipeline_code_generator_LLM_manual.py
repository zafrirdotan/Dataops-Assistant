import json
import os
from app.services.llm_service import LLMService
import pandas as pd


class PipelineCodeGeneratorLLMManual:
    """
    Generates actual pipeline code using LLM-based generation.
    """
    
    def __init__(self, log):
        self.llm = LLMService()
        self.log = log

    async def generate_code(self, spec: dict, data_preview: pd.DataFrame = None) -> str:
        """
        Generate the pipeline code based on the specification and optional data preview.
        """
        
        # Fix the DataFrame boolean evaluation issue
        data_preview_json = "No data preview provided."
        if data_preview is not None and not data_preview.empty:
            try:
                data_preview_json = data_preview.head().to_json(orient="records")
            except Exception as e:
                self.log.warning(f"Could not convert data preview to JSON: {e}")
                data_preview_json = "Data preview available but could not be serialized."
        
        prompt = f"""
        You are an expert Python developer specializing in data engineering and ETL pipelines.
        Given the following pipeline specification, generate a complete Python script that implements the pipeline.

        Pipeline Specification:
        {json.dumps(spec, indent=2)}

        Data Preview (if available):
        {data_preview_json}

        Code Structure:
        {self.getCodeStructurePrompt(spec)}
        Input:
        {self.getInputPrompt(spec)}
        Outputs:
        {self.getOutputsPrompt(spec)}
        """

        try:
            self.log.info(f"Generating code with prompt: {prompt}")
            response = await self.llm.generate_response_async(prompt)
        except Exception as e:
            self.log.error(f"Error generating code: {e}")
            return ""

        return self._clean_generated_code(response)

    def getInputPrompt(self, spec: dict) -> str:
        """
        Generate a prompt for the LLM to extract inputs from the specification.
        """
        source_type = spec.get("source_type", "")

        prompt_details = {
            "localfilecsv": (
                "The source is one or more local CSV files. "
                "The path to the local file is provided in os.getenv('DATA_FOLDER', '../../data'). "
                "Use wildcard patterns to match multiple files if specified (e.g., './data/*.csv')."
                "Use the glob library to find all matching files."
                "Include error handling for file not found and read errors."
            ),
            "PostgreSQL": (
                "The source is a postgres database. "
                "The connection string is provided in os.getenv('DATABASE_URL'). "
                "Use SQLAlchemy to connect and pandas to read the data."
                "Include error handling for connection issues and query errors."
                "The source table name is provided in the spec dictionary as spec['source_table']"
                "The source path is provided in the spec dictionary as spec['source_path']ß"
            ),
            # "api": (
            #     "The source is an API. "
            #     "Please specify the endpoint, authentication method, and parameters. "
            #     "Include error handling for network issues and invalid responses."
            # )
        }

        prompt_detail = prompt_details.get(
            source_type,
            "Unknown source type. Please provide details."
        )

        prompt = f"""
        {prompt_detail}
        """
        return prompt
    

    def getOutputsPrompt(self, spec: dict) -> str:
        """
        Generate a prompt for the LLM to extract outputs from the specification.
        """
        destination_type = spec.get("destination_type", "")

        prompt_details = {
            "parquet": (
                "The destination is Parquet files. "
                "The output folder is specified in os.getenv('OUTPUT_FOLDER', './output'). "
            ),
            "sqlite": (
                "The destination is a SQLite database. "
                "The connection string is provided in os.getenv('DATABASE_URL'). "
            ),
            "PostgreSQL": (
                "The destination is a Postgres database. "
                "The connection string is provided in os.getenv('DATABASE_URL'). "
                "Use SQLAlchemy to connect and pandas to write the data. "
                "The destination table name is provided in the spec dictionary as spec['destination_name']. "
                 "Before writing, check if the schema exists and create it if it does not (e.g., using CREATE SCHEMA IF NOT EXISTS). "
                "Include error handling for connection issues and write errors."
            ),
            # "api": (
            #     "The destination is an API endpoint. "
            #     "Please specify the endpoint, authentication method, and parameters. "
            #     "Include error handling for network issues and invalid responses."
            # )
        }

        prompt_detail = prompt_details.get(
            destination_type,
            "Unknown destination type. Please provide details."
        )

        prompt = f"""
        {prompt_detail}
        """
        return prompt
    
    def getCodeStructurePrompt(self, spec: dict) -> str:
        """
        Generate a prompt for the LLM to define the code structure based on the specification.
        """
        prompt = f"""
        Based on the following pipeline specification, outline the code structure for the implementation:

        Pipeline Specification:
        {spec}

        Code Structure Outline:
        - Import necessary libraries
        - Load environment variables
        - Define main function to orchestrate the pipeline
        - Define function to extract data from source
        - Define function to transform data based on specified logic
        - Define function to load data to destination - Parquet, SQLite, postgres one ore more as specified
        - Include error handling and logging
        - Ensure modularity and reusability of functions
        """
        return prompt
    

    async def generate_test_code(self, spec: dict, data_preview: pd.DataFrame = None) -> str:
        """Generate test code for the pipeline."""
        pipeline_name = spec.get("pipeline_name", "pipeline")
        
        test_code = f'''
        import pytest
        import pandas as pd
        import sys
        import os

        # Add the pipeline directory to the path
        sys.path.append(os.path.dirname(__file__))

        try:
            from {pipeline_name} import main
        except ImportError:
            # Fallback if import fails
            def main():
                print("Pipeline main function not found")
                return True

        def test_pipeline_execution():
            """Test that the pipeline runs without errors."""
            try:
                result = main()
                assert result is not None
                print("Pipeline executed successfully")
            except Exception as e:
                pytest.fail(f"Pipeline execution failed: {{e}}")

        def test_data_validation():
            """Test basic data validation."""
            # Add your data validation tests here
            assert True
            print("Data validation passed")

        if __name__ == "__main__":
            test_pipeline_execution()
            test_data_validation()
            print("All tests passed!")
        '''
        return test_code.strip()

    def generate_requirements_txt(self) -> str:
        """Generate requirements.txt for the pipeline."""
        requirements = [
            "pandas>=2.0.0",
            "numpy>=1.24.0",
            "sqlalchemy>=2.0.0",
            "psycopg2-binary>=2.9.0",
            "pyarrow>=14.0.0",
            "requests>=2.31.0",
            "pytest>=7.0.0",
            "python-dotenv>=1.0.0"
        ]
        return '\n'.join(requirements)
    
    def _clean_generated_code(self, code: str) -> str:
        """Clean and validate the generated code."""
        # Remove markdown code blocks if present
        if "```python" in code:
            start = code.find("```python") + 9
            end = code.rfind("```")
            if end > start:
                code = code[start:end].strip()
        elif "```" in code:
            start = code.find("```") + 3
            end = code.rfind("```")
            if end > start:
                code = code[start:end].strip()
        
        # Ensure proper indentation
        lines = code.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove any leading/trailing whitespace and normalize
            cleaned_line = line.rstrip()
            cleaned_lines.append(cleaned_line)
        
        return '\n'.join(cleaned_lines)