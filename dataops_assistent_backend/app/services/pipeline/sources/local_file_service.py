import os
import glob
import pandas as pd
import time
import asyncio

class LocalFileService:
    def __init__(self,log, data_directory=None):
        """Initialize with environment-aware data directory"""
        self.log = log

        if data_directory is None:
            # Use /app/data in Docker, ./data locally
            if os.path.exists("/app/data"):
                self.data_directory = "/app/data"
                self.log.info("Environment detected: Docker container - using /app/data")
            elif os.path.exists("../data"):
                self.data_directory = "../data"
                self.log.info("Environment detected: Local development - using ../data")
            else:
                # Fallback to current directory
                self.data_directory = "."
                self.log.info("Environment detected: Fallback - using current directory")
        else:
            self.data_directory = data_directory
            print(f"Using custom data directory: {self.data_directory}")
    
    def _resolve_pattern(self, file_pattern):
        """Resolve file pattern to work with the configured data directory"""
        # Handle absolute paths - return as is
        if os.path.isabs(file_pattern):
            self.log.info(f"Resolved pattern '{file_pattern}' to '{file_pattern}' (absolute path)")
            return file_pattern
        
        # Handle patterns that start with ./data/ or data/ - these should be treated as direct file references
        # Remove leading ./ if present
        clean_pattern = file_pattern.lstrip('./')
        
        # If pattern starts with 'data/', remove it since we're already in the data directory
        if clean_pattern.startswith('data/'):
            clean_pattern = clean_pattern[5:]  # Remove 'data/' prefix
        
        # Handle empty pattern after cleaning
        if not clean_pattern:
            clean_pattern = "*"
        
        # Join with data directory
        full_pattern = os.path.join(self.data_directory, clean_pattern)
        self.log.info(f"Resolved pattern '{file_pattern}' to '{full_pattern}'")
        return full_pattern

    def retrieve_recent_data_files(self, file_pattern, date_column=None, date_value=None):
        """ 
        Dynamically extract data from CSV or JSON files matching the pattern.
        Optionally filter by date_column and date_value.
        Returns a concatenated DataFrame of all matching rows.
        """
        try:
            full_pattern = self._resolve_pattern(file_pattern)
            files = glob.glob(full_pattern)
            now = time.time()
            last_24_hours = now - 24 * 60 * 60
            # recent_files = [f for f in files if os.path.getmtime(f) >= last_24_hours]
        except Exception as e:
            raise FileNotFoundError(f"Error accessing files for pattern: {file_pattern}. Details: {e}")

        # temp - all files
        # TODO - restore last_24_hours filter
        recent_files = files
        data_frames = []
        for file in recent_files:
            if file.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.endswith('.json'):
                df = pd.read_json(file)
            else:
                continue
            if date_column and date_value and date_column in df.columns:
                df = df[df[date_column] == date_value]
            data_frames.append(df)
        if data_frames:
            return pd.concat(data_frames, ignore_index=True)
        else:
            raise FileNotFoundError(f"No files found in last 24 hours for pattern: {file_pattern}")
        
    
    def check_file_exists(self, file_path):
        """
        Check if a file exists at the given path.
        """
        full_path = self._resolve_pattern(file_path)
        exists = len(glob.glob(full_path)) > 0
        print(f"Checking if '{file_path}' exists at '{full_path}': {exists}")
        return exists

    # Async methods
    async def retrieve_recent_data_files(self, file_pattern, date_column=None, date_value=None, limit=None):
        """
        Asynchronously extract data from CSV or JSON files matching the pattern.
        """
        return await asyncio.to_thread(
            self._retrieve_recent_data_files_sync, 
            file_pattern, 
            date_column, 
            date_value,
            limit
        )
    
    def _retrieve_recent_data_files_sync(self, file_pattern, date_column=None, date_value=None, limit=None):
        """Synchronous version for thread pool execution."""
        try:
            full_pattern = self._resolve_pattern(file_pattern)
            files = glob.glob(full_pattern)
            now = time.time()
            last_24_hours = now - 24 * 60 * 60
            # recent_files = [f for f in files if os.path.getmtime(f) >= last_24_hours]
        except Exception as e:
            raise FileNotFoundError(f"Error accessing files for pattern: {file_pattern}. Details: {e}")

        # temp - all files
        # TODO - restore last_24_hours filter
        recent_files = files
        data_frames = []
        for file in recent_files:
            if file.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.endswith('.json'):
                df = pd.read_json(file)
            else:
                continue
            if date_column and date_value and date_column in df.columns:
                df = df[df[date_column] == date_value]
            data_frames.append(df)
        if data_frames:
            if limit is not None:
                data_frames = data_frames[:limit]
            return pd.concat(data_frames, ignore_index=True)
        else:
            raise FileNotFoundError(f"No files found in last 24 hours for pattern: {file_pattern}")

    async def check_file_exists(self, file_path):
        """
        Asynchronously check if a file exists at the given path.
        """
        return await asyncio.to_thread(self._check_file_exists_sync, file_path)
    
    def _check_file_exists_sync(self, file_path):
        """Synchronous version for thread pool execution."""
        full_path = self._resolve_pattern(file_path)
        exists = len(glob.glob(full_path)) > 0
        print(f"Checking if '{file_path}' exists at '{full_path}': {exists}")
        return exists
