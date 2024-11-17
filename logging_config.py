import logging


def setup_logging(log_file="adbsim.log"):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),  # Optional: log to console
        ],
    )


def get_logger(name):
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())  # Ensure handlers exist
    return logger
