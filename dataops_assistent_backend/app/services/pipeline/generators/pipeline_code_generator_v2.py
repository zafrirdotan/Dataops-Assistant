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
        # Handle variations like "requirements.txt", "requirements", "txt", etc.
        if block_type == "requirements.txt":
            patterns = [
                r"```requirements\.txt(.*?)```",
                r"```requirements(.*?)```", 
                r"```txt(.*?)```",
                r"```\s*requirements\.txt\s*(.*?)```"
            ]
        else:
            patterns = [rf"```{block_type}(.*?)```"]
        
        for pattern in patterns:
            match = re.search(pattern, llm_response, re.DOTALL | re.IGNORECASE)
            if match:
                result = match.group(1).strip()
                if result:  # Only return non-empty results
                    return result
        return ""
    
    def check_requirements(self, requirements: str) -> bool | list:
        # Return True if all packages are allowed, otherwise False
        if not requirements.strip():
            return False
        lines = requirements.strip().split("\n")
        disallowed = []
        allowed_pkgs = [pkg.split(">=")[0].split("==")[0].strip().lower() for pkg in ALLOWED_PACKAGES]
        for line in lines:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            pkg_name = line.split(">=")[0].split("==")[0].strip().lower()
            if pkg_name and pkg_name not in allowed_pkgs:
                disallowed.append(line)
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
        
        prompt_v3 = f"""Generate COMPLETE, WORKING Python 3.13 ETL pipeline code.

Pipeline: {safe_json_dumps(spec, indent=2)}
Data: {safe_json_dumps(data_preview, indent=2)}

CRITICAL SUCCESS REQUIREMENTS:
1. ALL imports must be from allowed packages: {', '.join(ALLOWED_PACKAGES)}
2. Code must be COMPLETE - no TODO comments or incomplete functions
3. Handle ALL error cases with try/catch blocks
4. SQLAlchemy: Use proper data types (String, Integer, Float, Date, Text)
5. PostgreSQL: Always create schema first with "CREATE SCHEMA IF NOT EXISTS"
6. All file paths must use os.path.join() for cross-platform compatibility
7. Virtual environment will have all requirements.txt packages pre-installed

FILES TO CREATE:
- ../pipelines/{pipeline_name}/{pipeline_name}.py
- ../pipelines/{pipeline_name}/requirements.txt  
- ../pipelines/{pipeline_name}/{pipeline_name}_test.py

{input_config}
{output_config}

MANDATORY PATTERNS FOR SUCCESS:
- CSV wildcards: Use glob.glob with DATA_FOLDER environment variable
- DataFrame concat: Combine multiple CSV files with pd.concat and ignore_index=True
- Date column: Always ensure 'date' column exists, add if missing
- SQLite setup: Use create_engine with sqlite:/// URL and create tables
- Parquet partition: Use partition_cols=['year','month'] with pyarrow engine
- PostgreSQL schema: Create schema before table operations

COMMON FAILURE FIXES:
- Import order: sqlalchemy imports before pandas operations
- Data types: Convert all columns to appropriate types before database operations
- Missing directories: Use os.makedirs(path, exist_ok=True) 
- NaN handling: Use df.fillna() or .dropna() before database inserts
- Column names: Strip whitespace with df.columns = [c.strip() for c in df.columns]

MUST return exactly these 3 blocks (requirements.txt cannot be empty and MUST include pytest):

```python
# Main pipeline code here
```

```requirements.txt
pandas>=2.0.0
numpy>=1.24.0
python-dotenv>=1.0.0
pyarrow>=14.0.0
pytest>=7.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
```

```python test
# Test code here using pytest
```"""

        if last_code and last_error:
            prompt_v3 += f"""

CRITICAL ERROR CORRECTION:
Error: {last_error}
Previous code: {last_code}
Previous test: {python_test}

SPECIFIC FIXES NEEDED:
- Missing imports: Add all required imports at top of file
- Missing pytest: Must include pytest>=7.0.0 in requirements.txt
- Import errors: Check module names match exactly (e.g., 'sqlalchemy' not 'SQLAlchemy')
- Path errors: Use os.path.join() and os.makedirs(path, exist_ok=True)
- Data type errors: Convert DataFrame dtypes before database operations
- Schema errors: Create PostgreSQL schema before table operations
- Connection errors: Use proper environment variable names (DATABASE_URL)

Fix the specific error above and regenerate ALL 3 blocks completely."""

        return prompt_v3

    def _generate_input_config(self, spec: dict) -> str:
        """Generate input configuration based on source_type"""
        source_type = spec.get('source_type', '').lower()
        source_path = spec.get('source_path', 'input_file')
        
        if source_type in ['localfilecsv', 'localfilejson']:
            if '*' in source_path:
                return f"""Input ({source_type} MULTIPLE): Use glob.glob() with DATA_FOLDER, pattern: {source_path}, combine all files"""
            else:
                return f"""Input ({source_type}): Load from DATA_FOLDER env var, path: {source_path}"""
        
        elif source_type == 'postgresql':
            return f"""Input (PostgreSQL): Use DATABASE_URL env, table: {spec.get('source_table', 'source_table')}, chunked reading"""
        
        else:
            return f"""Input: File-based with DATA_FOLDER"""

    def _generate_output_config(self, spec: dict) -> str:
        """Generate output configuration based on destination_type"""
        dest_type = spec.get('destination_type', '').lower()
        transformation = spec.get('transformation', '').lower()
        
        # Check if transformation mentions multiple outputs
        has_parquet = 'parquet' in transformation
        has_sqlite = 'sqlite' in transformation or dest_type == 'sqlite'
        has_postgres = 'postgres' in transformation or dest_type == 'postgresql'
        
        if has_parquet and (has_sqlite or has_postgres):
            db_type = 'SQLite' if has_sqlite else 'PostgreSQL'
            dest_name = spec.get('destination_name', 'output_table')
            return f"""Output (DUAL - CRITICAL): Write to BOTH destinations in sequence:
1. Parquet: partition by year/month in output/ folder  
2. {db_type}: table '{dest_name}' using sqlalchemy with proper data types
BOTH outputs are mandatory - do not skip either one."""
        
        elif dest_type in ['localfilecsv', 'localfilejson', 'parquet']:
            return f"""Output ({dest_type}): Save to output/ folder, file: {spec.get('destination_name', 'output_file')}"""
        
        elif dest_type in ['sqlite', 'sqllite']:
            destination_name = spec.get('destination_name', spec.get('destination_table', 'output_table'))
            return f"""Output (SQLite): Use sqlalchemy, create table '{destination_name}', handle all data types properly"""
        
        elif dest_type == 'postgresql':
            destination_name = spec.get('destination_name', spec.get('destination_table', 'output_table'))
            if '.' in destination_name:
                schema, table = destination_name.split('.', 1)
                return f"""Output (PostgreSQL): 
1. Connect with DATABASE_URL environment variable
2. CREATE SCHEMA IF NOT EXISTS {schema}  
3. Create/replace table {schema}.{table} with proper column types
4. Use df.to_sql() with method='multi' for bulk insert
5. Handle schema creation BEFORE table operations"""
            else:
                return f"""Output (PostgreSQL): Use DATABASE_URL, table: {destination_name}, create table with proper types, bulk insert"""
        
        else:
            return f"""Output: File-based to output/ folder"""
    