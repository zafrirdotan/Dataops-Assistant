

from os import sync
import pandas as pd

from shared.utils.json_utils import make_json_serializable

from shared.services.database_service import get_database_service
from .local_file_service import LocalFileService

class SourceService:

    def __init__(self, log):

        self.log = log
        self.local_file_service = LocalFileService(self.log)
        self.database_service = get_database_service()

    async def fetch_data_from_source(self, spec: dict, limit: int) -> dict:
        # Try connecting to source/destination based on spec
        match spec.get("source_type"):
            case "PostgreSQL":
                data_preview = []
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
                    # Fetch a small sample of data for preview
                    data = await self.database_service.fetch_all(f"SELECT * FROM {table_name} LIMIT {limit}")
                   
                    # Convert to Json serializable format
                    if data is not None and len(data) > 0:
                        # Get column names
                        table_only = source_table.split('.')[-1]  # Extract table name without schema
                        schema_name = table_name.split('.')[0] if '.' in table_name else 'public'
                        columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_only}' AND table_schema = '{schema_name}' ORDER BY ordinal_position"
                        column_results = await self.database_service.fetch_all(columns_query)
                        columns = [{"name": row[0], "type": row[1]} for row in column_results] if column_results else None
                        
                        columns_names = [col['name'] for col in columns] if columns else None
                        # Convert to DataFrame for easier handling
                        df = pd.DataFrame(data, columns=columns_names if columns_names else None)
                        raw_preview = df.head().to_dict(orient="records")
                        # Make JSON serializable
                        data_preview = make_json_serializable(raw_preview)
                        self.log.debug(f"PostgreSQL data preview: {data_preview}")
                    else:
                        self.log.warning(f"No data found in table {table_name}")
                except Exception as e:
                    self.log.error(f"Error fetching data from {table_name}: {e}")
                    return {"success": False, "details": f"Error fetching data from table {table_name}: {e}"}
                
                return {"success": True, "data_preview": data_preview, "columns": columns}

            case "localFileCSV":
                try:
                    data = await self.local_file_service.retrieve_recent_data_files(spec.get("source_path"), date_column="event_date", date_value="2025-09-18", limit=limit)
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
                try:
                    data = await self.local_file_service.retrieve_recent_data_files(spec.get("source_path"), date_column="event_date", date_value="2025-09-18", limit=limit)
                    if data is not None:
                        raw_preview = data.head().to_dict(orient="records")
                        # Make JSON serializable
                        data_preview = make_json_serializable(raw_preview)
                        return {"success": True, "data_preview": data_preview}
                    else:
                        return {"success": False, "details": "No recent data files found."}
                except Exception as e:
                    return {"failed": False, "details": "Failed to connect to local JSON source."}
            case "sqlLite":
                pass
            case "api":
                pass

        return {"success": True}
    

    