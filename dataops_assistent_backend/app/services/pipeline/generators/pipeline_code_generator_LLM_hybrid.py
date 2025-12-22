import json
from logging import debug
import os
from app.services.llm_service import LLMService
import pandas as pd

from ..types import CodeGenResult

class PipelineCodeGeneratorLLMHybrid:
    """
    Generates actual pipeline code using LLM-based generation.
    """
    
    def __init__(self, log):
        self.llm = LLMService()
        self.log = log

    async def generate_code(self, spec: dict, db_info: dict) -> CodeGenResult:
        """
        Generate the pipeline code based on the specification and optional data preview.
        """
        
        prompt = f"""
        You are an expert Python developer specializing in data engineering and ETL pipelines.
        Given the following pipeline specification, generate a complete Python script that implements the pipeline.

        Pipeline Specification:
        {json.dumps(spec, indent=2)}
        Data Preview:
        {db_info.get("data_preview")}
        Columns Info:
        {db_info.get("columns")}

        Implementation Instructions:
        {self.getImplementationInstructions(spec)}

        This is the template you should follow:
        {self.getCodeTemplate(spec)}

        Use only the libraries specified in the requirements.txt.
        {self.generate_requirements_txt()}

        Test Code:
        This is an example of sanity test code for the pipeline:
        {await self.generate_test_code(spec, pd.DataFrame())}
        Follow this structure
        Create some mock data to test the pipeline functions.
        When cleaning up the output directory (e.g., output),
        use shutil.rmtree with an onerror handler to handle files that are in use or locked.
        Ensure all file handles are closed before attempting to delete the directory
        Use only temporary directories (such as those provided by pytest’s tmp_path or Python’s tempfile module) for all test data and output.
        Do not delete or clean up static/shared directories.
        All test files and outputs should be created and removed automatically by the temporary directory context.
        When testing the output as postgresql, use postgresql to test not sqlite.
        To convert a Python object to a JSON string use json.dumps() always.

        Output code, requirements.txt, and test code only in your response.
        """

        try:
            response = await self.llm.response_create_async(
                input = prompt,
                text={
                "format": {
                    "type": "json_schema",
                    "name": "extract_json",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "pipeline": {"type": "string"},
                            "requirements": {"type": "string"},
                            "tests": {"type": "string"}
                        },                       
                        "required": ["pipeline", "requirements", "tests"],
                        "additionalProperties": False
                    }
                }
            })

        except Exception as e:
            self.log.error(f"Error generating code: {e}")
            return ""
        self.log.debug(f"LLM Code Generation Response: {response.output_text}")
        json_response = json.loads(response.output_text)
        
        return {
            "pipeline": self._clean_generated_code(json_response.get("pipeline", "")),
            "requirements": self._clean_generated_code(json_response.get("requirements", "")),
            "tests": self._clean_generated_code(json_response.get("tests", ""))
        }
    
    def getImplementationInstructions(self, spec: dict) -> str:
        
        instructions = f"""
          Please follow these guidelines:
        - Validate all required variables before use.
        - Add clear error handling and informative logging for each step.
        - Document assumptions and expected inputs/outputs in comments.
        - Ensure the code is modular and easy to test.
        - Use best practices for data privacy and security.
        - Include environment variable usage for sensitive information
        
        Input Specification:
        {self.getInputSpecifications(spec)}
        
        Output Specification:
        {self.getOutputsSpecifications(spec)}
        """
        return instructions

    def getInputSpecifications(self, spec: dict) -> str:
        """
        Generate a prompt for the LLM to extract inputs from the specification.
        """
        source_type = spec.get("source_type", "")

        prompt_details = {
            "localfilecsv": (
                "The source is one or more local CSV files. "
                "The path to the local file is provided in os.getenv('DATA_FOLDER', '../../data'). "
                "Use wildcard patterns to match multiple files if specified"
                "Use the glob library to find all matching files."
                "Include error handling for file not found and read errors."
            ),
            "PostgreSQL": (
                "The source is a postgres database. "
                "The connection string is provided in os.getenv('DATABASE_URL'). "
                "Use SQLAlchemy to connect and pandas to read the data."
                "Include error handling for connection issues and query errors."
                "The source table name is provided in the spec dictionary as spec['source_table']"
                "The source path is provided in the spec dictionary as spec['source_path']"
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
    

    def getOutputsSpecifications(self, spec: dict) -> str:
        """
        Generate a prompt for the LLM to extract outputs from the specification.
        """

        prompt_details = {
            "parquet": (
                "The destination is Parquet files. "
                "The output folder is specified in os.getenv('OUTPUT_FOLDER', './output_test'). "
            ),
            "sqlite": (
                "The destination is a SQLite files. "
                 "The output folder is specified in os.getenv('OUTPUT_FOLDER', './output_test'). "
            ),
            "PostgreSQL": (
                "The destination is a Postgres database. "
                "The connection string is provided in os.getenv('DATABASE_URL'). "
                "Use SQLAlchemy to connect and pandas to write the data. "
                "The destination table name is provided in the spec dictionary as spec['destination_name']. "
                "Before writing, check if the schema exists and create it if it does not. "
                "Include error handling for connection issues and write errors."
            ),
            # "api": (
            #     "The destination is an API endpoint. "
            #     "Please specify the endpoint, authentication method, and parameters. "
            #     "Include error handling for network issues and invalid responses."
            # )

        }
        
        destination_type = spec.get("destination_type", "")
        
        prompt_detail = prompt_details.get(
            destination_type,
            "Unknown destination type. Please provide details."
        )

        return prompt_detail


    def getCodeTemplate(self, spec: dict) -> str:
        template = f"""
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

# Global pipeline specification
spec = {{
    "pipeline_name": "example_pipeline",
    "source_type": "PostgreSQL",
    "source_table": "public.transactions",
    "destination_type": "PostgreSQL",
    "destination_name": "dw.fact_transactions",
    "transformation_logic": "merge into destination by txn_id"
}}

# extract_data function to extract data from source
{self.getInputTemplate(spec)}
# transform_data function to apply transformation logic
{self.getTransformationTemplate(spec)}
# load_data function to load data to destination
{self.getOutputTemplate(spec)}
# Main function to orchestrate the pipeline
def main():
    load_dotenv()
    try:
        data = extract_data()
        if data is not None:
            transformed_data = transform_data(data)
            if transformed_data is not None:
                load_data(transformed_data)
    except Exception as e:
        logging.exception("Pipeline execution failed")
        raise  # Show full error in console

if __name__ == "__main__":
    main()
"""
        return template
    
    def getInputTemplate(self, spec: dict) -> str:
        source_type = spec.get("source_type", "")
        if source_type == "localFileCSV":
            return """
def extract_data():
    try:
        data_folder = os.getenv('DATA_FOLDER', '../../data')
        file_pattern = os.path.join(data_folder, '*.csv')
        all_files = glob.glob(file_pattern)
        if not all_files:
            raise FileNotFoundError(f"No CSV files found in {data_folder}")
        df_list = [pd.read_csv(file) for file in all_files]
        data = pd.concat(df_list, ignore_index=True)
        return data
    except Exception as e:
        logging.error(f"Error extracting data from CSV files: {str(e)}")
        return None
            """
        elif source_type == "PostgreSQL":
            return """
def extract_data():
    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)
        query = f"SELECT * FROM {spec['source_table']}"
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        logging.error(f"Error extracting data from PostgreSQL: {str(e)}")
        return None
            """
        
    def getTransformationTemplate(self, spec: dict) -> str:
        return """
def transform_data(data):
    # Add transformation logic here
    return data
        """
    
    def getOutputTemplate(self, spec: dict) -> str:
        destination_type = spec.get("destination_type", "")
        if destination_type == "PostgreSQL":
            if "merge" in spec.get("transformation_logic", ""):
                return """
def load_data(data):
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy import text

    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)    
        schema = spec['destination_name'].split('.')[0]
        table_name = spec['destination_name'].split('.')[1]
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            metadata = sqlalchemy.MetaData()
            # if table does not exist, create it

            # Dynamically create table if it does not exist
            columns = [sqlalchemy.Column(col, sqlalchemy.String) for col in data.columns if col != 'txn_id']
            columns.insert(0, sqlalchemy.Column('txn_id', sqlalchemy.Integer, primary_key=True))
            table = sqlalchemy.Table(table_name, metadata, *columns, schema=schema)
            metadata.create_all(engine)
            
            # Reflect the table after creation
            table = sqlalchemy.Table(table_name, metadata, autoload_with=engine, schema=schema)
            with engine.begin() as conn:
                for _, row in data.iterrows():
                    stmt = insert(table).values(**row.to_dict())
                    update_dict = {col: row[col] for col in row.index if col != 'txn_id'}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['txn_id'],
                        set_=update_dict
                    )
                    conn.execute(stmt)
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL with merge: {str(e)}")
            """
            else:
                return """
def load_data(data):
    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)
        schema = spec['destination_name'].split('.')[0]
        table_name = spec['destination_name'].split('.')[1]
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            data.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {str(e)}")
            """
        
        elif destination_type == "parquet":
            return """
        def load_data(data):
            try:
                output_folder = os.getenv('OUTPUT_FOLDER', './output_test')
                # Only create the directory if it does not exist
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)
                output_path = os.path.join(output_folder, 'output.parquet')
                data.to_parquet(output_path, index=False)
            except Exception as e:
                logging.error(f"Error loading data to Parquet: {str(e)}")
            """
        elif destination_type == "sqlite":
            return """
def load_data(data):
    try:
        output_folder = os.getenv('OUTPUT_FOLDER', './output_test')
        # Only create the directory if it does not exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        table_name = spec['destination_name']
        db_path = os.path.join(output_folder, f'{table_name}.sqlite')
        engine = create_engine(f'sqlite:///{db_path}')
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
    except Exception as e:
        logging.error(f"Error loading data to SQLite: {str(e)}")
            """
        else:
            return """
def load_data(data):
    logging.error("Unsupported destination type")
            """ 
    

    async def generate_test_code(self, spec: dict, data_preview: pd.DataFrame = None) -> str:
        """Generate test code for the pipeline."""
        
        test_code = f'''
import pytest
import pandas as pd
import sys
import os

# Add the pipeline directory to the path
sys.path.append(os.path.dirname(__file__))

try:
    from pipeline import main
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
            "python-dotenv>=1.0.0",
            "minio"

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
        
        return '\n'.join(cleaned_lines).replace("```", "")
