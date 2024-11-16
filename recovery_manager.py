from logging_config import get_logger

class RecoveryManager:
    def __init__(self):
        """Initialize the RecoveryManager."""
        self.logger = get_logger(self.__class__.__name__)
        self.log_file = "log.csv"
        self.logger.info("RecoveryManager initialized.")

    def write_log(self, transaction_id, operation, data_id=None, old_value=None, new_value=None):
        """
        Write an operation to the WAL log.
        - transaction_id: ID of the transaction performing the operation.
        - operation: The type of operation ('S', 'F', 'R', 'C').
        - data_id: ID of the data involved (if applicable).
        - old_value: The old value of the data (if applicable).
        - new_value: The new value of the data (if applicable).
        """
        log_entry = [transaction_id, operation]
        if data_id is not None:
            log_entry.extend([data_id, old_value, new_value])

        # Write to the log file
        with open(self.log_file, "a") as f:
            f.write(",".join(map(str, log_entry)) + "\n")
        self.logger.info(f"Log entry added: {log_entry}")

    def apply_logs(self):
        """
        Apply the WAL log to recover the database state.
        """
        self.logger.info("Applying WAL logs to recover database state.")
        try:
            with open(self.log_file, "r") as f:
                for line in f:
                    self.logger.debug(f"Processing log entry: {line.strip()}")
                    # Apply the log entry to the database
        except FileNotFoundError:
            self.logger.warning("Log file not found. Skipping recovery.")
