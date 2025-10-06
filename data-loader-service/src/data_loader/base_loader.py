from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class BaseLoader(ABC):
    """Base class for all data loaders"""
    
    def __init__(self, settings):
        self.settings = settings
    
    @abstractmethod
    def load_data(self):
        """Load data - to be implemented by subclasses"""
        pass
    
    def validate_data(self, data):
        """Validate loaded data"""
        if data is None or (hasattr(data, 'empty') and data.empty):
            raise ValueError("No data loaded")
        return True
