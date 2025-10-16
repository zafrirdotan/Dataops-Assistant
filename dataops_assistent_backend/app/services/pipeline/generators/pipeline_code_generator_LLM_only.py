import os
from app.services.llm_service import LLMService


class PipelineCodeGeneratorLLMOnly:
    """
    Generates actual pipeline code using LLM-based generation.
    """
    
    def __init__(self):
        self.llm = LLMService()

    async def generate_code(self, spec: dict, data_preview=None) -> str:
        """
        Generate pipeline code using LLM.
        
        Args:
            spec (dict): Pipeline specification from PipelineSpecGenerator
            data_preview: Sample of the actual data for better transformation logic
            
        Returns:
            str: Complete pipeline code ready to execute
        """
        # Create context for LLM
        context = self._build_context(spec, data_preview)
        
        # Generate the complete pipeline code using LLM
        prompt = self._create_pipeline_generation_prompt(context)
        pipeline_code = await self.llm.generate_response_async(prompt)
        
        # Clean and validate the generated code
        return self._clean_generated_code(pipeline_code)
    
    def _build_context(self, spec: dict, data_preview) -> dict:
        """Build context for LLM code generation."""
        context = {
            'pipeline_name': spec['pipeline_name'],
            'source_type': spec['source_type'],
            'source_path': spec.get('source_path', ''),
            'source_table': spec.get('source_table', ''),
            'destination_type': spec['destination_type'],
            'destination_name': spec['destination_name'],
            'transformation_logic': spec.get('transformation_logic', ''),
            'schedule': spec['schedule'],
            'data_sample': None,
            'data_columns': [],
            'data_types': {}
        }
        
        # Add data preview information if available
        if data_preview is not None and hasattr(data_preview, 'empty') and not data_preview.empty:
            context['data_sample'] = data_preview.head(3).to_dict()
            context['data_columns'] = list(data_preview.columns)
            context['data_types'] = {col: str(dtype) for col, dtype in data_preview.dtypes.items()}
        
        return context
    
    def _create_pipeline_generation_prompt(self, context: dict) -> str:
        """Create a comprehensive prompt for LLM pipeline generation."""
        
        data_info = ""
        if context['data_sample']:
            data_info = f"""
Data Preview Information:
- Columns: {context['data_columns']}
- Data Types: {context['data_types']}
- Sample Data: {context['data_sample']}
"""
        
        prompt = f"""
Generate a complete Python ETL pipeline with the following specifications:

Pipeline Details:
- Name: {context['pipeline_name']}
- Source Type: {context['source_type']}
- Source Path/Table: {context.get('source_path') or context.get('source_table')}
- Destination Type: {context['destination_type']}
- Destination Name: {context['destination_name']}
- Transformation Logic: {context['transformation_logic']}
- Schedule: {context['schedule']}

{data_info}

Requirements:
1. Generate a complete, executable Python script
2. Include all necessary imports (pandas, sqlalchemy, os, glob, etc.)
3. Create a main pipeline function named: {self._sanitize_function_name(context['pipeline_name'])}
4. Include proper error handling and logging
5. Add data validation and type optimization
6. Handle the specific source type: {context['source_type']}
7. Handle the specific destination type: {context['destination_type']}
8. Implement the transformation logic: {context['transformation_logic']}

CRITICAL - Analyze Transformation Logic for Merge Operations:
Look for these keywords in the transformation logic to detect merge/upsert requirements:
- "merge" / "upsert" / "update" / "sync"
- "by [column_name]" (indicates merge key)
- "into [table_name]" (indicates target table)
- Schema references like "schema.table"
- Phrases like "existing records", "conflict", "duplicate"

If merge operation detected:
- Generate UPSERT code with proper conflict handling
- Use the specified merge key column (e.g., customer_id, user_id)
- Add created_at/updated_at audit columns
- Use temporary staging table approach
- DO NOT use pandas if_exists='replace'

Source Type Guidelines:
- localFileCSV: Use pandas.read_csv with proper path handling and wildcard support if needed
- localFileJSON: Use pandas.read_json
- PostgreSQL: Use sqlalchemy and pandas.read_sql_table with proper schema handling
- api: Use requests library

Destination Type Guidelines:
- PostgreSQL: Use sqlalchemy with dynamic schema/table creation, proper indexing
- sqlite: Use sqlite3 with pandas.to_sql
- parquet: Use pandas.to_parquet

CRITICAL - Merge/UPSERT Operations:
If the transformation logic contains words like "merge", "upsert", "update", "by [column_name]", or "into [table_name]":

MANDATORY IMPLEMENTATION STEPS:
1. NEVER use if_exists='replace' - this destroys existing data
2. Use temporary staging table approach with proper UPSERT
3. Add audit columns (updated_at = CURRENT_TIMESTAMP, created_at for new records)
4. Use the specified merge key column exactly as mentioned in the prompt

REQUIRED MERGE CODE PATTERN - Generate this exact structure:
When merge/upsert operation is detected, generate the following helper function and usage:

1. Create upsert_to_target function in the generated code
2. Use temporary table staging approach  
3. Execute proper ON CONFLICT UPSERT with specified merge key
4. Add audit columns (updated_at, created_at)
5. Clean up temporary table after operation

Example structure to generate:
- def upsert_to_target(df, engine, target_schema, target_table, merge_key): ...
- Use temp table: temp_[table]_[timestamp] 
- SQL: INSERT INTO target SELECT * FROM temp ON CONFLICT (merge_key) DO UPDATE SET ...
- Call: upsert_to_target(df, engine, 'analytics', 'dim_customers', 'customer_id')

Environment Variables (use these in your code):
- DATA_FOLDER: os.getenv('DATA_FOLDER', '../../data') - for input data files
- OUTPUT_FOLDER: os.getenv('OUTPUT_FOLDER', './output') - for output files
- DATABASE_URL: os.getenv('DATABASE_URL', 'postgresql://dataops_user:dataops_password@localhost:5432/dataops_db') - for PostgreSQL connections

Code Structure:
```python
import pandas as pd
from sqlalchemy import create_engine, text
import os
import glob
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def {self._sanitize_function_name(context['pipeline_name'])}():
    \"\"\"
    Generated ETL Pipeline: {context['pipeline_name']}
    Schedule: {context['schedule']}
    \"\"\"
    try:
        print(f"Starting pipeline {context['pipeline_name']} at {{datetime.now()}}")
        
        # Environment configuration
        data_folder = os.getenv('DATA_FOLDER', '../../data')
        output_folder = os.getenv('OUTPUT_FOLDER', './output')
        database_url = os.getenv('DATABASE_URL', 'postgresql://dataops_user:dataops_password@localhost:5432/dataops_db')
        
        # Ensure output directory exists
        os.makedirs(output_folder, exist_ok=True)
        
        print(f"Using data folder: {{data_folder}}")
        print(f"Using output folder: {{output_folder}}")
        
        # Data Loading
        [GENERATE APPROPRIATE LOADER CODE USING data_folder]
        
        # Data Transformation
        df = transform_data(df)
        
        # Validate data before writing
        if df.empty:
            print("Warning: No data to write after transformations")
            return True
            
        print(f"Data validation: {{len(df)}} rows, {{len(df.columns)}} columns")
        
        # Data Writing - CRITICAL DECISION POINT:
        [IF "merge" OR "upsert" OR "by [column]" detected in transformation_logic:]
        [  GENERATE: upsert_to_target function with temp table staging]
        [  GENERATE: ON CONFLICT (merge_key) DO UPDATE logic]
        [  DO NOT USE: if_exists='replace' - this destroys existing data]
        [ELSE for regular loads:]
        [  USE: standard pandas.to_sql or file operations]
        
        print(f"Pipeline {context['pipeline_name']} completed successfully")
        return True
        
    except Exception as e:
        print(f"Pipeline failed: {{e}}")
        raise

def transform_data(df):
    \"\"\"Apply transformations to the dataframe.\"\"\"
    [GENERATE TRANSFORMATION CODE BASED ON LOGIC]
    
    # FOR MERGE OPERATIONS - Add audit columns:
    # Check transformation_logic for merge keywords and add timestamps
    # df['updated_at'] = pd.Timestamp.now()
    # if 'created_at' not in df.columns: df['created_at'] = pd.Timestamp.now()
    
    return df

if __name__ == "__main__":
    {self._sanitize_function_name(context['pipeline_name'])}()
```

Generate the complete, production-ready code with all placeholders filled in based on the specifications.
"""
        return prompt
    
    def _sanitize_function_name(self, pipeline_name: str) -> str:
        """Convert pipeline name to valid Python function name."""
        import re
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', pipeline_name)
        # Ensure it starts with a letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = f"pipeline_{sanitized}"
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove trailing underscores
        sanitized = sanitized.strip('_')
        return sanitized if sanitized else "pipeline_function"
    
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

    async def generate_test_code(self, spec: dict, data_preview=None) -> str:
        """Generate comprehensive test code for the pipeline using LLM."""
        
        context = self._build_context(spec, data_preview)
        
        prompt = f"""
Generate comprehensive pytest test code for the following ETL pipeline:

Pipeline Details:
- Name: {context['pipeline_name']}
- Source Type: {context['source_type']}
- Destination Type: {context['destination_type']}
- Transformation Logic: {context['transformation_logic']}

Data Information:
- Columns: {context['data_columns']}
- Data Types: {context['data_types']}

Requirements:
1. Generate complete pytest test suite
2. Include mocking for I/O operations based on source/destination types
3. Test transformation logic with realistic data
4. Test error handling scenarios
5. Test empty dataframe handling
6. Use proper pytest fixtures and assertions

Generate the following test structure:
```python
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from {context['pipeline_name']} import {self._sanitize_function_name(context['pipeline_name'])}, transform_data

class Test{self._sanitize_function_name(context['pipeline_name']).title()}:
    \"\"\"Test suite for {context['pipeline_name']} pipeline.\"\"\"
    
    [GENERATE ALL TEST METHODS]

if __name__ == "__main__":
    pytest.main([__file__])
```

Include tests for:
- transform_data function with realistic test data
- complete pipeline execution with mocked I/O
- error handling scenarios
- edge cases (empty data, missing columns, etc.)
"""
        
        test_code = await self.llm.generate_response_async(prompt)
        return self._clean_generated_code(test_code)

    async def generate_requirements_txt(self, spec: dict) -> str:
        """Generate requirements.txt for the pipeline using LLM."""
        
        prompt = f"""
Generate a requirements.txt file for a Python ETL pipeline with the following specifications:

Pipeline Details:
- Source Type: {spec['source_type']}
- Destination Type: {spec['destination_type']}
- Transformation Logic: {spec.get('transformation_logic', '')}

Base Requirements:
- pandas for data manipulation
- numpy for numerical operations
- pytest for testing
- python-dotenv for environment variable management

Additional Requirements Based on Components:
- If PostgreSQL: sqlalchemy, psycopg2-binary
- If API source: requests
- If parquet destination: pyarrow
- If Excel files: openpyxl
- If date/time operations: python-dateutil

Environment Configuration:
The generated pipeline should use environment variables from .env file:
- DATA_FOLDER: Input data directory path
- OUTPUT_FOLDER: Output directory path  
- DATABASE_URL: PostgreSQL connection string

Generate a complete requirements.txt with specific version numbers for stability.
Format as plain text with package>=version on each line.
"""
        
        requirements = await self.llm.generate_response_async(prompt)
        return self._clean_generated_code(requirements)

    async def generate_dockerfile(self, spec: dict) -> str:
        """Generate Dockerfile for the pipeline using LLM."""
        
        prompt = f"""
Generate a Dockerfile for a Python ETL pipeline with the following specifications:

Pipeline Details:
- Name: {spec['pipeline_name']}
- Source Type: {spec['source_type']}
- Destination Type: {spec['destination_type']}

Requirements:
1. Use official Python 3.11 slim image
2. Set working directory to /app
3. Copy and install requirements.txt
4. Copy pipeline code
5. Set appropriate environment variables
6. Handle data directory mounting
7. Set proper user permissions
8. Include health check if applicable

Generate a production-ready Dockerfile with best practices for security and performance.
"""
        
        dockerfile = await self.llm.generate_response_async(prompt)
        return self._clean_generated_code(dockerfile)

    async def generate_docker_compose(self, spec: dict) -> str:
        """Generate docker-compose.yml for the pipeline using LLM."""
        
        prompt = f"""
Generate a docker-compose.yml file for a Python ETL pipeline with the following specifications:

Pipeline Details:
- Name: {spec['pipeline_name']}
- Source Type: {spec['source_type']}
- Destination Type: {spec['destination_type']}
- Schedule: {spec['schedule']}

Requirements:
1. Include the pipeline service
2. Add PostgreSQL service if needed for source/destination
3. Include volume mounts for data directory
4. Set up environment variables
5. Configure networking between services
6. Add health checks
7. Include restart policies

Generate a complete docker-compose.yml v3.8+ with proper service definitions.
"""
        
        compose = await self.llm.generate_response_async(prompt)
        return self._clean_generated_code(compose)

    async def generate_env_file(self, spec: dict) -> str:
        """Generate .env file for the pipeline using LLM."""
        
        prompt = f"""
Generate a .env file for a Python ETL pipeline with the following specifications:

Pipeline Details:
- Name: {spec['pipeline_name']}
- Source Type: {spec['source_type']}
- Destination Type: {spec['destination_type']}

Requirements:
1. Include all necessary environment variables for the pipeline
2. Set appropriate default values
3. Include comments explaining each variable
4. Follow the structure from the .env.template

Required Environment Variables:
- DATA_FOLDER: Path to input data directory
- OUTPUT_FOLDER: Path to output directory
- DATABASE_URL: Full PostgreSQL connection string
- DATABASE_HOST: PostgreSQL host
- DATABASE_PORT: PostgreSQL port
- DATABASE_NAME: Database name
- DATABASE_USER: Database username
- DATABASE_PASSWORD: Database password

Generate a complete .env file with realistic default values.
"""
        
        env_content = await self.llm.generate_response_async(prompt)
        return self._clean_generated_code(env_content)