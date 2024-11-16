import logging
import os

def setup_logging(log_file="adbsim.log"):
    """Setup centralized logging configuration."""
    # Ensure the directory for the log file exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),  # Logs to file
            logging.StreamHandler()         # Logs to console
        ]
    )

def get_logger(name):
    """Get a logger with a specified name."""
    return logging.getLogger(name)
