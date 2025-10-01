"""
JSON utilities for handling serialization of complex Python objects.
"""

import json
import numpy as np
from datetime import datetime, date, time
from decimal import Decimal
import pandas as pd


def make_json_serializable(obj):
    """
    Convert data to JSON serializable format by handling common non-serializable types.
    
    Args:
        obj: The object to make JSON serializable (can be dict, list, or any value)
        
    Returns:
        JSON serializable version of the object
        
    Examples:
        >>> from datetime import date, time
        >>> data = {'date': date(2024, 1, 28), 'time': time(21, 56, 11)}
        >>> make_json_serializable(data)
        {'date': '2024-01-28', 'time': '21:56:11'}
        
        >>> import numpy as np
        >>> make_json_serializable([np.int64(42), np.float64(3.14)])
        [42, 3.14]
    """
    if isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, time):
        return obj.isoformat()
    elif pd.isna(obj) or obj is None or (isinstance(obj, float) and np.isnan(obj)):
        return None
    else:
        return obj


def safe_json_dumps(obj, **kwargs):
    """
    Safely serialize an object to JSON string, handling non-serializable types.
    
    Args:
        obj: The object to serialize
        **kwargs: Additional arguments to pass to json.dumps()
        
    Returns:
        JSON string representation of the object
        
    Examples:
        >>> from datetime import date
        >>> safe_json_dumps({'date': date(2024, 1, 28)}, indent=2)
        '{\n  "date": "2024-01-28"\n}'
    """
    serializable_obj = make_json_serializable(obj)
    return json.dumps(serializable_obj, **kwargs)


def safe_json_loads(json_str):
    """
    Safely load JSON string, with error handling.
    
    Args:
        json_str: JSON string to parse
        
    Returns:
        Parsed JSON object or None if parsing fails
    """
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError) as e:
        print(f"JSON parsing error: {e}")
        return None
