import os
import sys
import logging
from data_loader import DataLoaderManager
from config.settings import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for data initialization"""
    try:
        logger.info("Starting data loader service...")
        
        # Load configuration
        settings = get_settings()
        
        # Initialize data loader manager
        loader_manager = DataLoaderManager(settings)
        
        # Run data initialization
        loader_manager.initialize_all_data()
        
        logger.info("Data initialization completed successfully!")
        
    except Exception as e:
        logger.error(f"Data initialization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
