import json
from operator import is_
import os
from shared.services.llm_service import LLMService
import pandas as pd

from ..types import CodeGenResult

class PipelineCodeGeneratorLLMHybrid:
    """
    Generates actual pipeline code using LLM-based generation.
    """

    def __init__(self, log):
        self.llm = LLMService()
        self.log = log
        self.env_mode = os.getenv('ENVIRONMENT', 'dev')

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
        For assertions: NEVER use 'is' for value comparisons (e.g., 'is True'). Use == instead. DataFrame values are numpy types (np.True_/np.False_), not Python bool. Only use 'is' for None.
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


        if self.env_mode == 'prod':
            parquet_spec = (
                "The destination is Parquet files. "
                "Upload the output to S3 using boto3. "
                "S3 credentials are in environment variables: S3_BUCKET, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY. "
                "The S3 key should be: pipeline-outputs/pipeline_id/output.parquet"
            )
            sqlite_spec = (
                "The destination is a SQLite file. "
                "Create the SQLite file in a temporary location, then upload to S3 using boto3. "
                "S3 credentials are in environment variables: S3_BUCKET, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY. "
                "The S3 key should be: pipeline-outputs/pipeline_id/table_name.sqlite"
            )
        else:
            parquet_spec = (
                "The destination is Parquet files. "
                "The output folder is os.getenv('OUTPUT_FOLDER', './output')/pipeline_id/. "
            )
            sqlite_spec = (
                "The destination is a SQLite file. "
                "The output folder is os.getenv('OUTPUT_FOLDER', './output')/pipeline_id/. "
            )

        prompt_details = {
            "parquet": parquet_spec,
            "sqlite": sqlite_spec,
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


        # Generate environment-specific imports
        imports = """import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import glob"""
        is_prod = self.env_mode == 'prod'

        if is_prod:
            imports += "\nimport boto3\nfrom botocore.config import Config\nimport io"

        load_env = "load_dotenv('.env.prod')" if is_prod else "load_dotenv()"

        template = f"""
{imports}

# Load environment configuration
{load_env}

# Configure logging
PIPELINE_NAME = spec.get("pipeline_name", "unknown_pipeline")
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)s %(name)s [pipeline: {spec.get('pipeline_name', 'unknown_pipeline')}] %(message)s",
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
            if self.env_mode == 'prod':
                return """
def load_data(data):
    import tempfile
    try:
        metadata_path = os.path.join(os.path.dirname(__file__), 'metadata.json')
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        pipeline_id = metadata.get('pipeline_id')

        # S3 configuration
        s3_bucket = os.getenv('S3_BUCKET')
        s3_region = os.getenv('S3_REGION', os.getenv('AWS_REGION', 'us-east-1'))
        aws_access_key = os.getenv('S3_ACCESS_KEY')
        aws_secret_key = os.getenv('S3_SECRET_KEY')
        s3_endpoint = os.getenv('S3_ENDPOINT')
        s3_use_path_style = os.getenv('S3_USE_PATH_STYLE', 'false').lower() == 'true'

        endpoint_url = None if not s3_endpoint or s3_endpoint.lower() in ["", "none"] else s3_endpoint
        s3_client = boto3.client(
            's3',
            region_name=s3_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            endpoint_url=endpoint_url,
            config=Config(s3={"addressing_style": "path" if s3_use_path_style else "auto"})
        )

        # Write to temp file, then upload to S3
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            data.to_parquet(tmp_path, index=False)

            s3_key = f"pipeline-outputs/{pipeline_id}/output.parquet"
            s3_client.upload_file(tmp_path, s3_bucket, s3_key)
            logging.info(f"Data loaded to S3: s3://{s3_bucket}/{s3_key}")
            os.unlink(tmp_path)
    except Exception as e:
        logging.error(f"Error loading data to Parquet: {str(e)}")
            """
            else:
                return """
def load_data(data):
    try:
        metadata_path = os.path.join(os.path.dirname(__file__), 'metadata.json')
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        pipeline_id = metadata.get('pipeline_id')

        base_output = os.getenv('OUTPUT_FOLDER', './output')
        output_folder = os.path.join(base_output, pipeline_id)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_path = os.path.join(output_folder, 'output.parquet')
        data.to_parquet(output_path, index=False)
        logging.info(f"Data loaded to local file: {output_path}")
    except Exception as e:
        logging.error(f"Error loading data to Parquet: {str(e)}")
            """
        elif destination_type == "sqlite":
            # Check environment mode at generation time
            is_prod = self.env_mode == 'prod'

            if is_prod:
                return """
def load_data(data):
    import tempfile
    try:
        metadata_path = os.path.join(os.path.dirname(__file__), 'metadata.json')
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        pipeline_id = metadata.get('pipeline_id')
        table_name = spec['destination_name']

        # S3 configuration
        s3_bucket = os.getenv('S3_BUCKET')
        s3_region = os.getenv('S3_REGION', os.getenv('AWS_REGION', 'us-east-1'))
        aws_access_key = os.getenv('S3_ACCESS_KEY')
        aws_secret_key = os.getenv('S3_SECRET_KEY')
        s3_endpoint = os.getenv('S3_ENDPOINT')
        s3_use_path_style = os.getenv('S3_USE_PATH_STYLE', 'false').lower() == 'true'

        endpoint_url = None if not s3_endpoint or s3_endpoint.lower() in ["", "none"] else s3_endpoint
        s3_client = boto3.client(
            's3',
            region_name=s3_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            endpoint_url=endpoint_url,
            config=Config(s3={"addressing_style": "path" if s3_use_path_style else "auto"})
        )

        # Create SQLite in temp file and upload to S3
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp_file:
            db_path = tmp_file.name
            engine = create_engine(f'sqlite:///{db_path}')
            data.to_sql(table_name, con=engine, if_exists='replace', index=False)
            engine.dispose()

            s3_key = f"pipeline-outputs/{pipeline_id}/{table_name}.sqlite"
            s3_client.upload_file(db_path, s3_bucket, s3_key)
            logging.info(f"Data loaded to S3: s3://{s3_bucket}/{s3_key}")
            os.unlink(db_path)
    except Exception as e:
        logging.error(f"Error loading data to SQLite: {str(e)}")
            """
            else:
                return """
def load_data(data):
    try:
        metadata_path = os.path.join(os.path.dirname(__file__), 'metadata.json')
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        pipeline_id = metadata.get('pipeline_id')
        table_name = spec['destination_name']

        base_output = os.getenv('OUTPUT_FOLDER', './output')
        output_folder = os.path.join(base_output, pipeline_id)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        db_path = os.path.join(output_folder, f'{table_name}.sqlite')
        engine = create_engine(f'sqlite:///{db_path}')
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded to local file: {db_path}")
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

        if self.env_mode == 'prod':
            output_setup = """pipeline_id = pipeline.get_pipeline_id()
# In prod mode, output is in S3
output_folder = None"""
        else:
            output_setup = """pipeline_id = pipeline.get_pipeline_id()
base_output = os.getenv('OUTPUT_FOLDER', './output')
output_folder = os.path.join(base_output, pipeline_id)"""

        test_code = f'''
import pytest
import pandas as pd
import sys
import os
import pipeline


# Add the pipeline directory to the path
sys.path.append(os.path.dirname(__file__))

{output_setup}

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
            "pytest>=7.0.0",
            "python-dotenv>=1.0.0",
            "minio",
            "boto3>=1.26.0"

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
