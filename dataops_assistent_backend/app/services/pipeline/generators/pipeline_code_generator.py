import os
import re
from app.services.llm_service import LLMService
from app.utils.json_utils import safe_json_dumps

ALLOWED_PACKAGES = [
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "python-dotenv>=1.0.0",
    "pyarrow>=14.0.0",
    "pytest>=7.0.0",
    "sqlalchemy>=2.0.0",  # Add this for PostgreSQL support
    "psycopg2-binary>=2.9.0"  # Add this for PostgreSQL driver
]
class PipelineCodeGenerator:
    """
    Service for retrieving a data sample and generating transformation code using LLM.
    """
    def __init__(self):
        self.llm = LLMService()

    def generate_code(self, spec: dict, data_preview: dict, last_code: str = None, last_error: str = None, python_test: str = None) -> str:
        """
        Generate transformation code including data loading, transformation, and saving
        as well as unit tests.
        Args:
            spec (dict): The pipeline specification.
            db_info (dict): Information about the source/destination databases.
        Returns:
            str: Generated transformation code.
        """

        pipeline_name = spec.get("pipeline_name")

        # V3 Prompt with conditional configuration
        prompt_v3 = self._create_conditional_prompt(spec, data_preview, pipeline_name, last_code, last_error, python_test)

        response = self.llm.response_create(
            model="gpt-4.1",
            input=prompt_v3,  # Using v3 prompt with conditional configuration
            temperature=0,
        )
        
        # Simple try-catch approach for response handling
        try:
            response_text = response.output_text
        except AttributeError:
            # If output_text doesn't exist, treat response as string
            response_text = str(response) if response is not None else "Error: No response received"
        except Exception as e:
            print(f"Error processing LLM response: {e}")
            response_text = str(response) if response is not None else "Error: Failed to process response"

        python_code = self.extract_code_block(response_text, "python")
        requirements = self.extract_code_block(response_text, "requirements.txt")
        python_test = self.extract_code_block(response_text, "python test")

        check_result = self.check_requirements(requirements)
        if check_result != True:
            if check_result == False:
                raise ValueError("Generated requirements.txt is empty or invalid")
            else:
                raise ValueError(f"Generated requirements.txt contains disallowed packages: {check_result}")

        return python_code, requirements, python_test

    def extract_code_block(self, llm_response: str, block_type: str) -> str:
        # Extract code between triple backticks with block_type
        pattern = rf"```{block_type}(.*?)```"
        match = re.search(pattern, llm_response, re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""
    
    def check_requirements(self, requirements: str) -> bool | list:
        # Return True if all packages are allowed, otherwise False
        if not requirements.strip():
            return False
        lines = requirements.strip().split("\n")
        disallowed = []
        allowed_pkgs = [pkg.split(">=")[0].split("==")[0].strip().lower() for pkg in ALLOWED_PACKAGES]
        for line in lines:
            pkg_name = line.split(">=")[0].split("==")[0].strip().lower()
            if pkg_name and pkg_name not in allowed_pkgs:
                disallowed.append(line.strip())
        if disallowed:
            print(f"Disallowed packages found: {disallowed}")
            return disallowed
        return True

    def _create_conditional_prompt(self, spec: dict, data_preview: dict, pipeline_name: str, 
                                   last_code: str = None, last_error: str = None, python_test: str = None) -> str:
        """
        Create v3 prompt with conditional configuration based on source_type and destination_type
        """
        input_config = self._generate_input_config(spec)
        output_config = self._generate_output_config(spec)
        
        prompt_v3 = f"""
        ## CONTEXT
        You are an expert Python 3.13 engineer generating production-quality ETL pipeline code.

        **Pipeline Specification:**
        {safe_json_dumps(spec, indent=2)}

        **Data Preview:**
        {safe_json_dumps(data_preview, indent=2)}

        ## CONSTRAINTS
        - **Python Version:** 3.13 with best practices
        - **Allowed Packages:** {', '.join(ALLOWED_PACKAGES)}
        - **Code Quality:** Use type hints, modular structure, and error handling

        ## FILE STRUCTURE REQUIREMENTS
        - **Main Code:** `../pipelines/{pipeline_name}/{pipeline_name}.py`
        - **Requirements:** `../pipelines/{pipeline_name}/requirements.txt`
        - **Unit Test:** `../pipelines/{pipeline_name}/{pipeline_name}_test.py`
        - Ensure this folder exists before writing files
        - **Output Folder:** All output files must be saved to `../pipelines/{pipeline_name}/output/`
        
        ## DATA SOURCE CONFIGURATION
        {input_config}
        
        ## DATA OUTPUT CONFIGURATION
        {output_config}

        ## DATA HANDLING REQUIREMENTS
        ### ETL Processing
        - **Data Ingestion:** Process ALL available data regardless of records/partitions
        - **Date Column:** Ensure DataFrame has 'date' column (add today's date if missing)
        - **Parquet Partitioning:** 
          - Prefer year/month grouping to avoid system limits
          - Write without partitioning if too many unique dates

        ## TESTING REQUIREMENTS
        - **Framework:** pytest
        - **Import Pattern:** `from {pipeline_name} import ...`
        - **Coverage:** Test main transformation function for correctness

        ## OUTPUT FORMAT
        Return exactly three code blocks in this order:
        1. ```python
        [main pipeline code]
        ```
        2. ```requirements.txt
        [package dependencies]
        ```
        3. ```python test
        [unit test code]
        ```

        **Important:** Return ONLY the three code blocks. No explanations or extra text.
        """

        # Error handling section
        if last_code and last_error:
            prompt_v3 += f"""
            ## ERROR CORRECTION
            The previous code execution failed. Please fix the following:

            **Error:**
            {last_error}

            **Previous Code:**
            {last_code}

            **Previous Test:**
            {python_test}

            Fix the issues and regenerate all three code blocks.
            """

        return prompt_v3

    def _generate_input_config(self, spec: dict) -> str:
        """Generate input configuration based on source_type"""
        source_type = spec.get('source_type', '').lower()
        
        if source_type in ['localfilecsv', 'localfilejson']:
            return f"""
        ### Input Data Loading (File-based)
        ```python
        from dotenv import load_dotenv
        import os
        load_dotenv()
        DATA_FOLDER = os.getenv('DATA_FOLDER')
        ```
        - **Source Type:** {source_type}
        - **File Path:** Use `DATA_FOLDER` for all input file paths
        - **Data Validation:** Ensure file exists and format matches source type
        - **Error Handling:** Handle file not found, encoding issues, and malformed data
        - **Processing:** Ingest ALL available data regardless of file size
        - **File Pattern:** `os.path.join(DATA_FOLDER, '{spec.get('source_path', 'input_file')}')`
            """
        
        elif source_type == 'postgresql':
            return f"""
        ### Input Data Loading (PostgreSQL Database)
        ```python
        from dotenv import load_dotenv
        import os
        load_dotenv()
        DATABASE_URL = os.getenv('DATABASE_URL')
        DATABASE_HOST = os.getenv('DATABASE_HOST')
        DATABASE_PORT = os.getenv('DATABASE_PORT')
        DATABASE_NAME = os.getenv('DATABASE_NAME')
        DATABASE_USER = os.getenv('DATABASE_USER')
        DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
        ```
        - **Source Type:** PostgreSQL Database
        - **Connection:** Use DATABASE_URL: `{os.getenv('DATABASE_URL', 'postgresql://dataops_user:dataops_password@localhost:5432/dataops_db')}`
        - **Alternative Connection:** Individual parameters (HOST: {os.getenv('DATABASE_HOST', 'localhost')}, PORT: {os.getenv('DATABASE_PORT', '5432')})
        - **Authentication:** DATABASE_USER and DATABASE_PASSWORD from environment
        - **Query:** Extract data from table: `{spec.get('source_table', 'source_table')}`
        - **Connection Pattern:** Use SQLAlchemy or psycopg2 with proper connection pooling
        - **Error Handling:** Handle connection failures, timeouts, and SQL errors
        - **Processing:** Use chunked reading for large datasets with `chunksize` parameter
            """
        
        else:
            return f"""
        ### Input Data Loading (Generic)
        - **Source Type:** {source_type or 'Not specified'}
        - **Default:** Use file-based loading with DATA_FOLDER
        - **Note:** Consider specifying source_type as 'localFileCSV', 'localFileJSON', or 'PostgreSQL'
            """

    def _generate_output_config(self, spec: dict) -> str:
        """Generate output configuration based on destination_type"""
        dest_type = spec.get('destination_type', '').lower()
        
        if dest_type in ['localfilecsv', 'localfilejson', 'parquet']:
            return f"""
        ### Output Data Saving (File-based)
        ```python
        OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER')
        output_path = f'../pipelines/{spec.get('pipeline_name')}/output/'
        ```
        - **Destination Type:** {dest_type}
        - **Output Path:** Save to `../pipelines/{spec.get('pipeline_name')}/output/`
        - **Environment:** Use OUTPUT_FOLDER from .env if available
        - **File Management:** Ensure output directory exists before writing
        - **Naming:** Use descriptive filenames with timestamps if needed
        - **Partitioning:** For Parquet, use year/month grouping to avoid system limits
        - **File Pattern:** `os.path.join(output_path, '{spec.get('destination_name', 'output_file')}')`
            """
        
        elif dest_type == 'postgresql':
            destination_name = spec.get('destination_name', spec.get('destination_table', 'output_table'))
            schema_name = None
            table_name = destination_name
            
            # Extract schema if format is "schema.table"
            if '.' in destination_name:
                schema_name, table_name = destination_name.split('.', 1)
            
            return f"""
        ### Output Data Saving (PostgreSQL Database)
        ```python
        # Use same database configuration as input
        DATABASE_URL = os.getenv('DATABASE_URL')
        ```
        - **Destination Type:** PostgreSQL Database  
        - **Connection:** Use same DATABASE_URL and connection parameters as input
        - **Target Table:** `{destination_name}`
        - **Schema Management:** 
          {f"- CREATE SCHEMA IF NOT EXISTS `{schema_name}`" if schema_name else "- Use default schema"}
          - Create table if not exists with proper column types
          - Handle schema and table creation before data insertion
        - **Write Mode:** Handle table creation, updates, or appends as specified
        - **Data Types:** Ensure proper mapping from DataFrame to SQL types
        - **Batch Processing:** Use efficient bulk insert methods (`to_sql` with `method='multi'`)
        - **Transaction Handling:** Use database transactions for data integrity
        - **Error Handling:** Handle schema/table creation errors gracefully
        - **Important:** Always create schema first, then table, then insert data
            """
        
        else:
            return f"""
        ### Output Data Saving (Generic)
        - **Destination Type:** {dest_type or 'Not specified'}
        - **Default:** Save to file-based output in pipeline directory
        - **Path:** `../pipelines/{spec.get('pipeline_name')}/output/`
            """
    