import logging

def setup_logging(level=logging.INFO, log_format=None, handlers=None):
    """
    Set up logging for the application. Call this once at app startup.
    Args:
        level: Logging level (default: logging.INFO)
        log_format: Log message format (default: timestamp, level, message)
        handlers: List of logging handlers (default: StreamHandler to stdout)
    Returns:
        logger: The root logger instance
    """
    if log_format is None:
        log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    if handlers is None:
        handlers = [logging.StreamHandler()]
    logging.basicConfig(level=level, format=log_format, handlers=handlers, force=True)
    logger = logging.getLogger("dataops")
    logger.setLevel(level)
    return logger
