# Configure logging
import logging


def setup_logger(name, log_file):
    """
    Sets up and returns a logger with the specified name and configuration.

    Args:
        name (str): The name of the logger (e.g., module name).
        log_file (str): Path to the log file.


    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    # File handler for writing logs to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Stream handler for logging to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Add handlers to the logger
    if not logger.handlers:  # Prevent adding duplicate handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
