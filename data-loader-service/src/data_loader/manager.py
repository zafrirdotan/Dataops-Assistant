import logging
from typing import List
from .csv_loader import CSVLoader
from .database_loader import DatabaseLoader

logger = logging.getLogger(__name__)

class DataLoaderManager:
    """Manages all data loading operations"""
    
    def __init__(self, settings):
        self.settings = settings
        self.loaders = []
        self._initialize_loaders()
    
    def _initialize_loaders(self):
        """Initialize all available data loaders"""
        self.loaders = [
            DatabaseLoader(self.settings),  # Create tables first
            CSVLoader(self.settings)        # Then load CSV data
        ]
    
    def initialize_all_data(self):
        """Run all data loaders"""
        logger.info("Starting data initialization process...")
        
        for loader in self.loaders:
            try:
                logger.info(f"Running {loader.__class__.__name__}...")
                loader.load_data()
                logger.info(f"{loader.__class__.__name__} completed successfully")
            except Exception as e:
                logger.error(f"Error in {loader.__class__.__name__}: {e}")
                # Continue with other loaders
        
        logger.info("Data initialization process completed")
