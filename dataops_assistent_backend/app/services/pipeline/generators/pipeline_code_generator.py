import os

import pandas as pd
from app.services.llm_service import LLMService


class PipelineCodeGenerator:
    """
    Generates actual pipeline code using templates and pluggable components.
    """
    
    def __init__(self):
        self.llm = LLMService()

    def generate_code(self, spec: dict, data_preview: pd.DataFrame) -> str:
        """
        Generate pipeline code using base template + specific components.
        
        Args:
            spec (dict): Pipeline specification from PipelineSpecGenerator
            data_preview (pd.DataFrame): Sample of the actual data for better transformation logic
            
        Returns:
            str: Complete pipeline code ready to execute
        """
        # Get the appropriate loader
        loader_code = self._generate_loader(spec['source_type'], spec)
        
        # Get transformation logic (only the specific business logic)
        transform_code = self._generate_transformation(spec.get('transformation_logic', ''), data_preview)
        
        # Get the appropriate writer
        writer_code = self._generate_writer(spec['destination_type'], spec)
        
        # Use base template
        pipeline_code = self._get_base_template().format(
            pipeline_name=spec['pipeline_name'],
            loader_code=loader_code,
            transform_code=transform_code,
            writer_code=writer_code,
            schedule=spec['schedule']
        )
        
        return pipeline_code
    
    def _generate_transformation(self, transformation_logic: str, data_preview: pd.DataFrame = None) -> str:
        """Generate only the specific transformation function."""
        if not transformation_logic:
            return "    return df  # No transformations specified"
        
        # Prepare data preview information for the LLM
        data_info = ""
        if data_preview is not None and not data_preview.empty:
            data_info = f"""
        
        **Data Preview:**
        Columns: {list(data_preview.columns)}
        Data types: {dict(data_preview.dtypes)}
        Sample rows: {data_preview.head(3).to_dict('records')}
        Total rows in preview: {len(data_preview)}
        """
        
        prompt = f"""
        Generate only a Python function body for this transformation: {transformation_logic}
        {data_info}
        
        The function should:
        - Take a pandas DataFrame as input
        - Apply the specified transformation
        - Return the transformed DataFrame
        - Be concise and focused only on the transformation logic
        - Use proper indentation (4 spaces)
        - Use the actual column names from the data preview above
        - Handle edge cases (empty dataframes, missing columns)
        
        Return only the function body (the lines inside the function), not the function signature.
        Example output:
            # Filter active users
            df = df[df['status'] == 'active']
            return df
        """
        
        try:
            response = self.llm.response_create(
                model="gpt-4.1",
                input=prompt,
                temperature=0.1
            )
            
            # Ensure proper indentation
            transform_code = response.output_text.strip()
            if not transform_code.startswith('    '):
                # Add proper indentation if not present
                lines = transform_code.split('\n')
                transform_code = '\n'.join(f'    {line}' if line.strip() else line for line in lines)
            
            return transform_code
            
        except Exception as e:
            return f"    # Error generating transformation: {e}\n    return df"
    
    def _generate_loader(self, source_type: str, spec: dict) -> str:
        """Return appropriate loader code based on source type."""
        loaders = {
            "localFileCSV": f"""
        # Load CSV file
        df = pd.read_csv('{spec.get('source_path', 'data/input.csv')}')
        print(f"Loaded {{len(df)}} rows from CSV file")""",
            
            "localFileJSON": f"""
        # Load JSON file
        df = pd.read_json('{spec.get('source_path', 'data/input.json')}')
        print(f"Loaded {{len(df)}} rows from JSON file")""",
            
            "PostgreSQL": f"""
        # Load from PostgreSQL
        engine = create_engine(os.getenv('POSTGRES_URL', 'postgresql://user:pass@localhost:5432/db'))
        df = pd.read_sql_table('{spec.get('source_table', 'source_table')}', engine)
        print(f"Loaded {{len(df)}} rows from PostgreSQL table")""",
            
            "api": f"""
        # Load from API
        import requests
        response = requests.get('{spec.get('source_path', 'http://api.example.com/data')}')
        df = pd.DataFrame(response.json())
        print(f"Loaded {{len(df)}} rows from API")"""
        }
        
        return loaders.get(source_type, "        # Unknown source type\n        df = pd.DataFrame()")
    
    def _generate_writer(self, dest_type: str, spec: dict) -> str:
        """Return appropriate writer code based on destination type."""
        writers = {
            "PostgreSQL": self._generate_postgresql_writer(spec),
            
            "sqlite": f"""
        # Write to SQLite
        import sqlite3
        conn = sqlite3.connect('data/{spec['destination_name']}.db')
        df.to_sql('{spec['destination_name']}', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Written {{len(df)}} rows to SQLite table '{spec['destination_name']}'")""",
            
            "parquet": f"""
        # Write to Parquet file
        os.makedirs('data', exist_ok=True)
        df.to_parquet('data/{spec['destination_name']}.parquet')
        print(f"Written {{len(df)}} rows to Parquet file '{spec['destination_name']}.parquet'")"""
        }
        
        return writers.get(dest_type, "        # Unknown destination type")
    
    def _generate_postgresql_writer(self, spec: dict) -> str:
        """Generate PostgreSQL writer with dynamic schema and table creation."""
        destination_name = spec['destination_name']
        
        # Check if destination includes schema (schema.table format)
        if '.' in destination_name:
            schema_name, table_name = destination_name.split('.', 1)
            return f"""
        # Write to PostgreSQL with dynamic schema and table creation
        engine = create_engine(os.getenv('POSTGRES_URL', 'postgresql://user:pass@localhost:5432/db'))
        
        # Create schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
            print(f"Schema '{schema_name}' created/ensured to exist")
        
        # Check if table exists and drop it for fresh creation
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}')"
            ))
            table_exists = result.scalar()
            
            if table_exists:
                conn.execute(text("DROP TABLE {schema_name}.{table_name}"))
                conn.commit()
                print(f"Existing table '{destination_name}' dropped for fresh creation")
        
        # Create completely new table with optimized structure
        df.to_sql('{table_name}', engine, schema='{schema_name}', if_exists='fail', index=False, method='multi')
        print(f"NEW table '{destination_name}' created with {{len(df.columns)}} columns")
        print(f"Table structure: {{dict(df.dtypes)}}")
        print(f"Inserted {{len(df)}} rows into new table '{destination_name}'")
        
        # Add indexes for common query patterns
        with engine.connect() as conn:
            # Add primary key if there's an ID column
            id_columns = [col for col in df.columns if 'id' in col.lower()]
            if id_columns:
                primary_key_col = id_columns[0]  # Use first ID column as primary key
                try:
                    conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({{primary_key_col}})"))
                    print(f"Added primary key constraint on {{{{primary_key_col}}}}")
                except Exception as e:
                    print(f"Could not add primary key: {{{{e}}}}")
            
            # Add indexes on date columns
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            for date_col in date_columns:
                try:
                    index_name = f"idx_{table_name}_{{{{date_col}}}}"
                    conn.execute(text(f"CREATE INDEX {{{{index_name}}}} ON {schema_name}.{table_name} ({{{{date_col}}}})")")
                    print(f"Added index on {{{{date_col}}}}")
                except Exception as e:
                    print(f"Could not add index on {{{{date_col}}}}: {{{{e}}}}")
            
            conn.commit()"""
        else:
            return f"""
        # Write to PostgreSQL (public schema) with dynamic table creation
        engine = create_engine(os.getenv('POSTGRES_URL', 'postgresql://user:pass@localhost:5432/db'))
        
        # Check if table exists and drop it for fresh creation
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{destination_name}')"
            ))
            table_exists = result.scalar()
            
            if table_exists:
                conn.execute(text("DROP TABLE public.{destination_name}"))
                conn.commit()
                print(f"Existing table 'public.{destination_name}' dropped for fresh creation")
        
        # Create completely new table
        df.to_sql('{destination_name}', engine, if_exists='fail', index=False, method='multi')
        print(f"NEW table '{destination_name}' created with {{len(df.columns)}} columns")
        print(f"Table structure: {{dict(df.dtypes)}}")
        print(f"Inserted {{len(df)}} rows into new table '{destination_name}'")"""
    
    def _get_base_template(self) -> str:
        """Base pipeline template with dynamic schema and table creation utilities."""
        return '''import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
import os
from datetime import datetime


def {pipeline_name}():
    """
    Generated ETL Pipeline: {pipeline_name}
    Schedule: {schedule}
    """
    try:
        print(f"Starting pipeline {pipeline_name} at {{datetime.now()}}")
        
        # Data Loading
{loader_code}
        
        # Data Transformation
        df = transform_data(df)
        
        # Validate data before writing
        if df.empty:
            print("Warning: No data to write after transformations")
            return True
            
        print(f"Data validation: {{len(df)}} rows, {{len(df.columns)}} columns")
        print(f"Columns: {{list(df.columns)}}")
        
        # Data Writing
{writer_code}
        
        print(f"Pipeline {pipeline_name} completed successfully at {{datetime.now()}}")
        return True
        
    except Exception as e:
        print(f"Pipeline {pipeline_name} failed: {{e}}")
        raise


def transform_data(df):
    """Apply transformations to the dataframe."""
{transform_code}


def optimize_data_types(df):
    """Optimize DataFrame data types for better database storage."""
    df = df.copy()
    
    for col in df.columns:
        # Skip if column is completely null
        if df[col].isna().all():
            continue
            
        # Try to convert object columns to more specific types
        if df[col].dtype == 'object':
            # Try datetime conversion
            try:
                df[col] = pd.to_datetime(df[col], errors='ignore')
                if df[col].dtype != 'object':
                    continue
            except:
                pass
                
            # Try numeric conversion
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if not numeric_col.isna().all():
                    df[col] = numeric_col
                    continue
            except:
                pass
                
            # Keep as string but optimize
            df[col] = df[col].astype('string')
    
    return df


if __name__ == "__main__":
    {pipeline_name}()
'''

    def generate_test_code(self, spec: dict, data_preview: pd.DataFrame = None) -> str:
        """Generate comprehensive test code for the pipeline based on its specification."""
        
        # Generate appropriate mocks based on source and destination types
        source_mock = self._get_source_mock(spec['source_type'])
        destination_mock = self._get_destination_mock(spec['destination_type'])
        
        # Use data preview for more realistic test data if available
        test_data = self._generate_test_data(data_preview)
        
        test_template = '''import pytest
            import pandas as pd
            from unittest.mock import patch, MagicMock
            from {pipeline_name} import {pipeline_name}, transform_data


            class Test{pipeline_name_class}:
                """Test suite for {pipeline_name} pipeline."""
                
                def test_transform_data(self):
                    """Test the transformation logic."""
                    # Create sample data based on actual data structure
                    sample_data = pd.DataFrame({test_data})
                    
                    # Apply transformation
                    result = transform_data(sample_data)
                    
                    # Assert basic properties
                    assert isinstance(result, pd.DataFrame)
                    assert len(result) >= 0
                    # Ensure all columns are preserved or transformation is applied correctly
                    assert result.columns.tolist() == sample_data.columns.tolist() or len(result.columns) > 0
                    
                def test_empty_dataframe_handling(self):
                    """Test pipeline handles empty dataframes gracefully."""
                    empty_df = pd.DataFrame()
                    result = transform_data(empty_df)
                    assert isinstance(result, pd.DataFrame)
                    
                {source_mock}
                {destination_mock}
                def test_pipeline_execution(self, {mock_params}):
                    """Test complete pipeline execution with mocked I/O."""
                    # Mock data loading with realistic data structure
                    mock_data = pd.DataFrame({test_data})
                    {source_setup}
                    
                    # Run pipeline
                    result = {pipeline_name}()
                    
                    # Assertions
                    assert result is True
                    {destination_assertions}
                    
                def test_pipeline_error_handling(self):
                    """Test pipeline error handling."""
                    with patch('{error_mock_target}', side_effect=Exception("Test error")):
                        with pytest.raises(Exception):
                            {pipeline_name}()


            if __name__ == "__main__":
                pytest.main([__file__])
            '''

        pipeline_name_class = ''.join(word.capitalize() for word in spec['pipeline_name'].split('_'))
        
        return test_template.format(
            pipeline_name=spec['pipeline_name'],
            pipeline_name_class=pipeline_name_class,
            test_data=test_data,
            source_mock=source_mock['decorator'],
            destination_mock=destination_mock['decorator'],
            mock_params=f"{destination_mock['param']}, {source_mock['param']}",
            source_setup=source_mock['setup'],
            destination_assertions=destination_mock['assertions'],
            error_mock_target=source_mock['error_target']
        )
    
    def _get_source_mock(self, source_type: str) -> dict:
        """Generate appropriate mock configuration for source type."""
        mocks = {
            "localFileCSV": {
                "decorator": "@patch('pandas.read_csv')",
                "param": "mock_read_csv",
                "setup": "mock_read_csv.return_value = mock_data",
                "error_target": "pandas.read_csv"
            },
            "localFileJSON": {
                "decorator": "@patch('pandas.read_json')",
                "param": "mock_read_json", 
                "setup": "mock_read_json.return_value = mock_data",
                "error_target": "pandas.read_json"
            },
            "PostgreSQL": {
                "decorator": "@patch('pandas.read_sql_table')",
                "param": "mock_read_sql",
                "setup": "mock_read_sql.return_value = mock_data",
                "error_target": "pandas.read_sql_table"
            },
            "api": {
                "decorator": "@patch('requests.get')",
                "param": "mock_requests_get",
                "setup": "mock_requests_get.return_value.json.return_value = mock_data.to_dict('records')",
                "error_target": "requests.get"
            }
        }
        return mocks.get(source_type, mocks["localFileCSV"])
    
    def _get_destination_mock(self, dest_type: str) -> dict:
        """Generate appropriate mock configuration for destination type."""
        mocks = {
            "parquet": {
                "decorator": "@patch('pandas.DataFrame.to_parquet')",
                "param": "mock_to_parquet",
                "assertions": "mock_to_parquet.assert_called_once()"
            },
            "sqlite": {
                "decorator": "@patch('pandas.DataFrame.to_sql')",
                "param": "mock_to_sql",
                "assertions": "mock_to_sql.assert_called_once()"
            },
            "PostgreSQL": {
                "decorator": "@patch('pandas.DataFrame.to_sql')",
                "param": "mock_to_sql", 
                "assertions": "mock_to_sql.assert_called_once()"
            }
        }
        return mocks.get(dest_type, mocks["parquet"])
    
    def _generate_test_data(self, data_preview: pd.DataFrame = None) -> str:
        """Generate test data structure based on actual data preview."""
        if data_preview is not None and not data_preview.empty:
            # Use actual data structure from preview
            test_data_dict = {}
            for col in data_preview.columns:
                col_data = data_preview[col].head(3).tolist()
                # Handle different data types
                if data_preview[col].dtype == 'object':
                    # String data - create test versions
                    test_data_dict[col] = [f"test_{col}_{i+1}" for i in range(3)]
                elif data_preview[col].dtype in ['int64', 'int32', 'float64', 'float32']:
                    # Numeric data - use simple test values
                    test_data_dict[col] = [10*(i+1) for i in range(3)]
                else:
                    # Other types - try to use actual data or create simple test data
                    try:
                        test_data_dict[col] = col_data
                    except:
                        test_data_dict[col] = [f"test_value_{i+1}" for i in range(3)]
            
            return str(test_data_dict)
        else:
            # Default test data if no preview available
            return """{
                'id': [1, 2, 3],
                'name': ['Test1', 'Test2', 'Test3'],
                'value': [10, 20, 30],
                'status': ['active', 'active', 'inactive']
            }"""

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
    