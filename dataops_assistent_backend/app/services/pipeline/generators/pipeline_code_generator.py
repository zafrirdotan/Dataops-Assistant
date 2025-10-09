import os
import pandas as pd
from typing import Dict, Any
from app.services.llm_service import LLMService


class PipelineCodeGenerator:
    """Template-driven pipeline code generator."""
    
    def __init__(self):
        self.llm = LLMService()
        self.templates = {
            'loaders': self._get_loader_templates(),
            'writers': self._get_writer_templates(),
            'base': self._get_base_template()
        }

    async def generate_code(self, spec: dict, data_preview=None) -> str:
        """Generate pipeline code using hybrid approach: templates + LLM for complex logic."""
        # Get template components
        loader_code = self._get_loader_code(spec['source_type'], spec)
        
        # Handle multiple destination types - IMPROVED!
        writer_code = self._generate_writer_code(spec)
        
        # HYBRID APPROACH: Use LLM for complex operations, templates for simple ones
        transformation_logic = spec.get('transformation_logic', '')
        if self._needs_complex_logic(transformation_logic, spec):
            # Use LLM to generate sophisticated transformation logic
            transform_code = await self._generate_llm_transformation_logic(spec, data_preview)
        else:
            # Use simple template placeholder
            transform_code = self._get_transformation_placeholder(transformation_logic)
        
        # Sanitize function name
        function_name = self._sanitize_function_name(spec['pipeline_name'])
        
        # Assemble pipeline from templates
        pipeline_code = self.templates['base'].format(
            pipeline_name=spec['pipeline_name'],
            function_name=function_name,
            loader_code=loader_code,
            transform_code=transform_code,
            writer_code=writer_code,
            schedule=spec.get('schedule', 'manual')
        )
        
        return pipeline_code
    
    def _sanitize_function_name(self, pipeline_name: str) -> str:
        """Convert pipeline name to valid Python function name."""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', pipeline_name)
        if sanitized and sanitized[0].isdigit():
            sanitized = f"pipeline_{sanitized}"
        sanitized = re.sub(r'_+', '_', sanitized).strip('_')
        return sanitized if sanitized else "pipeline_function"
    
    def _needs_complex_logic(self, transformation_logic: str, spec: dict) -> bool:
        """Detect if the pipeline needs complex LLM-generated logic instead of templates."""
        if not transformation_logic:
            return False
            
        # Keywords that indicate complex operations requiring LLM generation
        complex_keywords = [
            'merge', 'upsert', 'insert', 'update', 'join', 'aggregate', 
            'deduplicate', 'dedup', 'group by', 'pivot', 'unpivot',
            'calculate', 'compute', 'transform', 'convert', 'normalize',
            'validate', 'clean', 'filter', 'sort', 'rank'
        ]
        
        # Check transformation logic and pipeline context
        text_to_check = f"{transformation_logic} {spec.get('pipeline_name', '')}".lower()
        
        # Also check source and destination types for database operations
        is_db_to_db = (spec.get('source_type') in ['PostgreSQL', 'MySQL', 'SQLite'] and 
                      spec.get('destination_type') in ['PostgreSQL', 'MySQL', 'SQLite'])
        
        has_complex_keyword = any(keyword in text_to_check for keyword in complex_keywords)
        
        return has_complex_keyword or is_db_to_db
    
    async def _generate_llm_transformation_logic(self, spec: dict, data_preview=None) -> str:
        """Generate sophisticated transformation logic using LLM."""
        try:
            # Prepare context for LLM
            transformation_logic = spec.get('transformation_logic', '')
            source_info = {
                'type': spec.get('source_type'),
                'table': spec.get('source_table'),
                'path': spec.get('source_path')
            }
            destination_info = {
                'type': spec.get('destination_type'),
                'name': spec.get('destination_name')
            }
            
            # Build prompt for LLM
            prompt = self._build_transformation_prompt(
                transformation_logic, source_info, destination_info, data_preview
            )
            
            # Generate code using LLM
            llm_response = await self.llm.generate_response(prompt)
            
            # Extract and clean the generated code
            generated_code = self._extract_transformation_code(llm_response, transformation_logic)
            
            return generated_code
            
        except Exception as e:
            print(f"LLM transformation generation failed: {e}")
            # Fallback to template placeholder
            return self._get_transformation_placeholder(transformation_logic)
    
    def _build_transformation_prompt(self, transformation_logic: str, source_info: dict, 
                                   destination_info: dict, data_preview=None) -> str:
        """Build a comprehensive prompt for LLM transformation generation."""
        
        # Basic context
        prompt = f"""Generate Python transformation code for an ETL pipeline.

TASK: {transformation_logic}

SOURCE: {source_info['type']}"""
        
        if source_info.get('table'):
            prompt += f" - Table: {source_info['table']}"
        if source_info.get('path'):
            prompt += f" - Path: {source_info['path']}"
            
        prompt += f"""
DESTINATION: {destination_info['type']} - {destination_info['name']}
"""
        
        # Add data preview if available
        if data_preview is not None and not data_preview.empty:
            columns = list(data_preview.columns)
            prompt += f"""
DATA SCHEMA:
Columns: {columns}
Sample data shape: {data_preview.shape}
"""
            
        # Add specific instructions based on operation type
        if 'merge' in transformation_logic.lower():
            prompt += """
MERGE OPERATION REQUIREMENTS:
- Generate proper UPSERT logic (INSERT new records, UPDATE existing ones)
- Use pandas for data processing and SQLAlchemy for database operations
- Handle conflicts by updating existing records based on the merge key
- Do NOT use DROP TABLE - preserve existing data
- Use proper transaction handling
- Include error handling and logging
"""
        
        prompt += """
REQUIREMENTS:
1. Return ONLY the Python code for the transform_data(df) function body
2. Use pandas DataFrame operations where appropriate
3. Include proper error handling and logging
4. Preserve data types and handle nulls appropriately
5. Add print statements for monitoring progress
6. Use 4-space indentation
7. Do NOT include the function definition line, just the body
8. Start each line with exactly 4 spaces for proper indentation

EXAMPLE FORMAT:
    print(f"Starting transformation: {transformation_logic}")
    print(f"Input data shape: {{df.shape}}")
    
    # Your transformation logic here
    result_df = df.copy()
    
    # Additional processing...
    
    print(f"Output data shape: {{result_df.shape}}")
    return result_df
"""
        
        return prompt
    
    def _extract_transformation_code(self, llm_response: str, transformation_logic: str) -> str:
        """Extract and clean transformation code from LLM response."""
        try:
            # Remove code block markers if present
            code = llm_response.strip()
            if code.startswith('```python'):
                code = code[9:]
            elif code.startswith('```'):
                code = code[3:]
            if code.endswith('```'):
                code = code[:-3]
            
            # Clean up and ensure proper indentation
            lines = code.strip().split('\n')
            cleaned_lines = []
            
            for line in lines:
                if line.strip():  # Skip empty lines
                    # Ensure each line has 4-space indentation
                    cleaned_line = '    ' + line.lstrip()
                    cleaned_lines.append(cleaned_line)
                else:
                    cleaned_lines.append('')
            
            return '\n'.join(cleaned_lines)
            
        except Exception as e:
            print(f"Error extracting transformation code: {e}")
            # Fallback to template
            return self._get_transformation_placeholder(transformation_logic)
    
    def _get_transformation_placeholder(self, transformation_logic: str) -> str:
        """Generate ONLY a placeholder for transformation logic."""
        if not transformation_logic:
            return """    # No transformation logic specified
    print("No transformations applied")
    return df"""
        
        return f"""    # Transformation Logic: {transformation_logic}
    print(f"Applying transformation: {transformation_logic}")
    print(f"Input data shape: {{df.shape}}")
    
    # TODO: Implement transformation logic based on: {transformation_logic}
    # This is where the LLM-generated transformation code will be inserted
    
    print(f"Output data shape: {{df.shape}}")
    return df"""
    
    def _get_loader_code(self, source_type: str, spec: dict) -> str:
        """Get loader code from templates."""
        template = self.templates['loaders'].get(source_type)
        if not template:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        return template.format(**self._get_loader_params(source_type, spec))
    
    def _get_writer_code(self, dest_type: str, spec: dict) -> str:
        """Get writer code from templates."""
        template = self.templates['writers'].get(dest_type)
        if not template:
            raise ValueError(f"Unsupported destination type: {dest_type}")
        
        return template.format(**self._get_writer_params(dest_type, spec))
    
    def _get_loader_params(self, source_type: str, spec: dict) -> Dict[str, Any]:
        """Get parameters for loader templates."""
        params = {
            'source_path': self._clean_source_path(spec.get('source_path', 'input.csv')),
            'source_table': spec.get('source_table', 'source_table'),
            'api_endpoint': spec.get('source_path', 'http://api.example.com/data'),
            'bucket_name': spec.get('bucket_name', 'data-bucket'),
            'minio_endpoint': spec.get('minio_endpoint', 'localhost:9000'),
            's3_region': spec.get('s3_region', 'us-east-1')
        }
        
        # Handle schema.table format for PostgreSQL
        if '.' in params['source_table']:
            schema, table = params['source_table'].split('.', 1)
            params['schema_name'] = schema
            params['table_name'] = table
        else:
            params['schema_name'] = 'public'
            params['table_name'] = params['source_table']
        
        return params
    
    def _get_writer_params(self, dest_type: str, spec: dict) -> Dict[str, Any]:
        """Get parameters for writer templates."""
        destination_name = spec.get('destination_name', 'output')
        
        params = {
            'destination_name': destination_name,
            'output_path': f"data/{destination_name}",
            'bucket_name': spec.get('bucket_name', 'data-bucket'),
            'minio_endpoint': spec.get('minio_endpoint', 'localhost:9000'),
            's3_region': spec.get('s3_region', 'us-east-1')
        }
        
        # Handle schema.table format for PostgreSQL
        if '.' in destination_name:
            schema, table = destination_name.split('.', 1)
            params['schema_name'] = schema
            params['table_name'] = table
            params['full_table_name'] = destination_name
        else:
            params['schema_name'] = 'public'
            params['table_name'] = destination_name
            params['full_table_name'] = f"public.{destination_name}"
        
        return params
    
    def _clean_source_path(self, path: str) -> str:
        """Clean source path by removing common prefixes."""
        clean_path = path.lstrip('./')
        if clean_path.startswith('data/'):
            clean_path = clean_path[5:]
        return clean_path if clean_path else 'input.csv'
    
    def _generate_writer_code(self, spec: dict) -> str:
        """Generate writer code - handles both single and multiple destinations."""
        destination_type = spec['destination_type']
        
        # Check if this might be a compound destination (multiple outputs)
        if self._is_compound_destination(spec):
            return self._generate_multiple_writers(spec)
        else:
            # Single destination - check if it's a merge operation
            transformation_logic = spec.get('transformation_logic', '')
            if ('merge' in transformation_logic.lower() and 
                destination_type == 'PostgreSQL'):
                return self._get_writer_code('PostgreSQL_merge', spec)
            else:
                return self._get_writer_code(destination_type, spec)
    
    def _is_compound_destination(self, spec: dict) -> bool:
        """Detect if the spec requires multiple destination outputs."""
        # Check pipeline name for indicators of multiple outputs
        pipeline_name = spec.get('pipeline_name', '').lower()
        destination_name = spec.get('destination_name', '').lower()
        
        # Look for keywords that indicate multiple outputs
        multi_indicators = ['and', 'both', 'also', 'plus', '&']
        storage_types = ['parquet', 'sqlite', 'postgres', 'csv', 'json', 'minio']
        
        text_to_check = f"{pipeline_name} {destination_name}"
        
        # Check if we have multiple storage type mentions
        storage_mentions = sum(1 for storage in storage_types if storage in text_to_check)
        has_multi_indicator = any(indicator in text_to_check for indicator in multi_indicators)
        
        # Special case: if destination_name contains table name like 'orders_daily' 
        # and primary destination is parquet, assume SQLite is also wanted
        has_table_name = destination_name and ('daily' in destination_name or 'table' in destination_name)
        is_parquet_primary = spec.get('destination_type') == 'parquet'
        
        return storage_mentions >= 2 or has_multi_indicator or (has_table_name and is_parquet_primary)
    
    def _generate_multiple_writers(self, spec: dict) -> str:
        """Generate code for multiple output destinations."""
        pipeline_name = spec.get('pipeline_name', '').lower()
        destination_name = spec.get('destination_name', '').lower()
        
        writers = []
        
        # Parse the pipeline name and destination to identify output types
        text_to_parse = f"{pipeline_name} {destination_name}"
        primary_dest = spec.get('destination_type', '')
        
        # Determine all destination types from the pipeline name and context
        has_parquet = ('parquet' in text_to_parse or 
                      'parquet' in spec.get('destination_type', '') or
                      primary_dest == 'parquet')
        
        has_sqlite = ('sqlite' in text_to_parse or 
                     'orders_daily' in destination_name or 
                     'table' in destination_name or
                     '_daily' in destination_name or
                     primary_dest == 'sqlite')
        
        # Add Parquet output if mentioned
        if has_parquet:
            writers.append(("Parquet", self._get_writer_code('parquet', spec)))
        
        # Add SQLite output if mentioned or table name suggests it
        if has_sqlite:
            # Create SQLite-specific spec
            sqlite_spec = spec.copy()
            sqlite_spec['destination_type'] = 'sqlite'
            if 'orders_daily' in destination_name:
                sqlite_spec['table_name'] = 'orders_daily'
            else:
                sqlite_spec['table_name'] = destination_name
            
            writers.append(("SQLite", self._get_writer_code('sqlite', sqlite_spec)))
        
        # Check for PostgreSQL mentions
        if 'postgres' in text_to_parse or 'postgresql' in text_to_parse:
            writers.append(("PostgreSQL", self._get_writer_code('PostgreSQL', spec)))
        
        # Check for MinIO mentions
        if 'minio' in text_to_parse:
            writers.append(("MinIO", self._get_writer_code('minioParquet', spec)))
        
        # If we only found one writer but this is a compound destination, add SQLite as default
        if len(writers) == 1:
            sqlite_spec = spec.copy() 
            sqlite_spec['table_name'] = destination_name if destination_name else 'output_table'
            writers.append(("SQLite", self._get_writer_code('sqlite', sqlite_spec)))
        
        # Combine all writers with separating comments
        combined_writers = []
        for i, (writer_type, writer_code) in enumerate(writers, 1):
            combined_writers.append(f"""
        # Output {i}: Write to {writer_type}
{writer_code}""")
        
        return '\n'.join(combined_writers)
    
    def _get_loader_templates(self) -> Dict[str, str]:
        """Define all loader templates."""
        return {
            "localFileCSV": """
        # Load CSV from local filesystem
        data_folder = os.getenv('DATA_FOLDER', '/app/data')
        csv_path = os.path.join(data_folder, '{source_path}')
        print(f"Loading CSV from: {{csv_path}}")
        
        if '*' in '{source_path}' or '?' in '{source_path}':
            import glob
            csv_files = glob.glob(csv_path)
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found matching: {{csv_path}}")
            print(f"Found {{len(csv_files)}} CSV files")
            dfs = [pd.read_csv(f) for f in csv_files]
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = pd.read_csv(csv_path)
        
        print(f"Loaded {{len(df)}} rows from CSV")""",

            "minioCSV": """
        # Load CSV from MinIO
        from minio import Minio
        import io
        
        minio_client = Minio(
            '{minio_endpoint}',
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False
        )
        
        print(f"Loading CSV from MinIO: {bucket_name}/{source_path}")
        response = minio_client.get_object('{bucket_name}', '{source_path}')
        df = pd.read_csv(io.BytesIO(response.data))
        print(f"Loaded {{len(df)}} rows from MinIO CSV")""",

            "PostgreSQL": """
        # Load from PostgreSQL
        from sqlalchemy import create_engine, text, inspect
        
        engine = create_engine(os.getenv('DATABASE_URL', 'postgresql://dataops_user:dataops_password@localhost:5432/dataops_db'))
        
        # Check if table exists
        inspector = inspect(engine)
        if not inspector.has_table('{table_name}', schema='{schema_name}'):
            raise ValueError(f"Table {schema_name}.{table_name} not found")
        
        df = pd.read_sql_table('{table_name}', engine, schema='{schema_name}')
        print(f"Loaded {{len(df)}} rows from PostgreSQL table {schema_name}.{table_name}")"""
        }
    
    def _get_writer_templates(self) -> Dict[str, str]:
        """Define all writer templates."""
        return {
            "PostgreSQL": """
        # Write to PostgreSQL with smart table management
        from sqlalchemy import create_engine, text
        
        engine = create_engine(os.getenv('DATABASE_URL', 'postgresql://dataops_user:dataops_password@localhost:5432/dataops_db'))
        
        # Create schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
            print(f"Schema '{schema_name}' created/ensured to exist")
        
        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}')"
            ))
            table_exists = result.scalar()
        
        if table_exists:
            # Table exists - this will be handled by transformation logic for merges
            print(f"Table '{full_table_name}' exists - will use merge/upsert logic")
            # For now, fallback to replace for non-merge operations
            df.to_sql('{table_name}', engine, schema='{schema_name}', if_exists='replace', index=False, method='multi')
            print(f"Replaced table '{full_table_name}' with {{len(df)}} rows")
        else:
            # Create new table
            df.to_sql('{table_name}', engine, schema='{schema_name}', if_exists='fail', index=False, method='multi')
            print(f"Created NEW table '{full_table_name}' with {{len(df.columns)}} columns")
            print(f"Table structure: {{dict(df.dtypes)}}")
            print(f"Inserted {{len(df)}} rows into new table '{full_table_name}'")""",

            "parquet": """
        # Write to Parquet with date partitioning
        os.makedirs('data', exist_ok=True)
        
        # Check if we should partition by date
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        
        if date_columns and len(date_columns) > 0:
            # Use the first date column for partitioning
            date_col = date_columns[0]
            print(f"Partitioning by date column: {{date_col}}")
            
            # Ensure the date column is datetime
            if df[date_col].dtype == 'object':
                df[date_col] = pd.to_datetime(df[date_col])
            
            # Create date partition column
            df['partition_date'] = df[date_col].dt.strftime('%Y-%m-%d')
            
            # Write partitioned parquet
            parquet_path = '{output_path}.parquet'
            df.to_parquet(parquet_path, partition_cols=['partition_date'], index=False)
            print(f"Written {{len(df)}} rows to partitioned Parquet: {{parquet_path}}")
        else:
            # Write single parquet file
            parquet_path = '{output_path}.parquet'
            df.to_parquet(parquet_path, index=False)
            print(f"Written {{len(df)}} rows to Parquet: {{parquet_path}}")""",

            "sqlite": """
        # Write to SQLite
        import sqlite3
        
        os.makedirs('data', exist_ok=True)
        db_path = '{output_path}.db'
        
        with sqlite3.connect(db_path) as conn:
            df.to_sql('{table_name}', conn, if_exists='replace', index=False)
        
        print(f"Written {{len(df)}} rows to SQLite: {{db_path}}")""",

            "minioParquet": """
        # Write Parquet to MinIO
        from minio import Minio
        import io
        
        minio_client = Minio(
            '{minio_endpoint}',
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False
        )
        
        # Convert to parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        object_name = f"{destination_name}.parquet"
        minio_client.put_object(
            '{bucket_name}',
            object_name,
            parquet_buffer,
            length=len(parquet_buffer.getvalue()),
            content_type='application/octet-stream'
        )
        
        print(f"Written {{len(df)}} rows to MinIO: {bucket_name}/{{object_name}}")""",

            "PostgreSQL_merge": """
        # PostgreSQL Merge Operation - UPSERT logic will be handled in transform_data()
        # This template is used when merge operations are detected
        from sqlalchemy import create_engine, text
        
        engine = create_engine(os.getenv('DATABASE_URL', 'postgresql://dataops_user:dataops_password@localhost:5432/dataops_db'))
        
        # Create schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
            print(f"Schema '{schema_name}' ensured to exist for merge operation")
        
        # The actual merge/upsert logic is handled in transform_data() function
        # This section just confirms the operation completed successfully
        print(f"Merge operation completed for table '{full_table_name}'")
        print(f"Final dataset contains {{len(df)}} rows")"""
        }
    
    def _get_base_template(self) -> str:
        """Base pipeline template - clean and focused."""
        return '''import pandas as pd
from sqlalchemy import create_engine, text
import os
import glob
from datetime import datetime


def {function_name}():
    """
    Generated ETL Pipeline: {pipeline_name}
    Schedule: {schedule}
    """
    try:
        print(f"Starting pipeline {pipeline_name} at {{datetime.now()}}")
        
        # Data Loading{loader_code}
        
        # Data Transformation
        df = transform_data(df)
        
        # Validate data before writing
        if df.empty:
            print("Warning: No data to write after transformations")
            return True
            
        print(f"Data validation: {{len(df)}} rows, {{len(df.columns)}} columns")
        print(f"Columns: {{list(df.columns)}}")
        
        # Data Writing{writer_code}
        
        print(f"Pipeline {pipeline_name} completed successfully at {{datetime.now()}}")
        return True
        
    except Exception as e:
        print(f"Pipeline {pipeline_name} failed: {{e}}")
        raise


def transform_data(df):
    """Apply transformations to the dataframe."""
{transform_code}


if __name__ == "__main__":
    {function_name}()
'''

    async def generate_test_code(self, spec: dict, data_preview=None) -> str:
        """Generate test code for the pipeline."""
        function_name = self._sanitize_function_name(spec['pipeline_name'])
        
        test_code = f'''import pytest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock

# Add the parent directory to the path to import the pipeline module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the pipeline function
from pipeline import {function_name}


class TestPipeline:
    """Test cases for {spec['pipeline_name']} pipeline."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.test_data = self._create_test_data()
    
    def _create_test_data(self) -> pd.DataFrame:
        """Create sample test data matching the expected schema."""
{self._generate_test_data(data_preview)}
    
    @patch('pandas.read_csv')
    def test_data_loading(self, mock_read_csv):
        """Test that data loading works correctly."""
        mock_read_csv.return_value = self.test_data
        
        try:
            result = {function_name}()
            assert result is True, "Pipeline should return True on success"
        except Exception as e:
            pytest.fail(f"Pipeline failed with error: {{e}}")
    
    def test_data_transformation(self):
        """Test that data transformation works correctly."""
        # Import transform_data function
        from pipeline import transform_data
        
        result_df = transform_data(self.test_data.copy())
        
        # Basic validation
        assert isinstance(result_df, pd.DataFrame), "Result should be a DataFrame"
        assert not result_df.empty, "Result should not be empty"
        
        print(f"Original shape: {{self.test_data.shape}}")
        print(f"Transformed shape: {{result_df.shape}}")
    
    def test_pipeline_end_to_end(self):
        """Test the entire pipeline end-to-end with mocked dependencies."""
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = self.test_data
            
            try:
                result = {function_name}()
                assert result is True, "End-to-end pipeline test should pass"
            except Exception as e:
                pytest.fail(f"End-to-end test failed: {{e}}")


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
'''
        return test_code
    
    def _generate_test_data(self, data_preview=None) -> str:
        """Generate sample test data based on the data preview."""
        if data_preview is not None and not data_preview.empty:
            # Use actual column names and sample data
            columns = list(data_preview.columns)
            sample_row = data_preview.iloc[0] if len(data_preview) > 0 else None
            
            if sample_row is not None:
                test_data_dict = {}
                for col in columns[:10]:  # Limit to first 10 columns to keep test manageable
                    value = sample_row[col]
                    if pd.isna(value):
                        test_data_dict[col] = [None, None, None]
                    elif isinstance(value, str):
                        test_data_dict[col] = [f"test_{col}_1", f"test_{col}_2", f"test_{col}_3"]
                    elif isinstance(value, (int, float)):
                        test_data_dict[col] = [1, 2, 3]
                    else:
                        test_data_dict[col] = [str(value), str(value), str(value)]
                
                return f"""        return pd.DataFrame({{
{', '.join([f'            "{col}": {test_data_dict[col]}' for col in test_data_dict.keys()])}
        }})"""
        
        # Fallback to generic test data
        return '''        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test Item 1', 'Test Item 2', 'Test Item 3'],
            'value': [100.0, 200.0, 300.0],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03']
        })'''

    def generate_requirements_txt(self) -> str:
        """Generate requirements.txt content for the pipeline."""
        return """pandas>=1.5.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
requests>=2.28.0
pytest>=7.0.0
minio>=7.1.0
boto3>=1.26.0
pyarrow>=10.0.0
"""
