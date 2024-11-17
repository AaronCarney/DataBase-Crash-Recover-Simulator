import csv
from logging_config import get_logger
import os

class RecoveryManager:
    def __init__(self, log_file="log.csv"):
        """Initialize the RecoveryManager."""
        self.logger = get_logger(self.__class__.__name__)
        self.log_file = log_file
        self.write_count = 0  # Track the number of writes since the last flush
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
        with open(self.log_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(log_entry)

        self.logger.info(f"Log entry added: {log_entry}")
        self.write_count += 1

        # Flush logs every 25 write operations
        if self.write_count >= 25:
            self.flush_logs()

    def flush_logs(self):
        """
        Flush log entries to ensure durability.
        (Currently, this is a no-op as logs are flushed after every write.)
        """
        self.logger.info("Logs flushed to disk.")
        self.write_count = 0  # Reset the write count

    def read_log(self):
        """
        Read and parse the WAL log.
        Returns:
            List of log entries (each entry is a list).
        """
        if not os.path.exists(self.log_file):
            self.logger.warning(f"Log file {self.log_file} does not exist. No logs to read.")
            return []

        log_entries = []
        with open(self.log_file, "r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                log_entries.append(row)
        self.logger.info(f"Read {len(log_entries)} log entries from the log file.")
        return log_entries

    def apply_logs(self, db_handler):
        """
        Apply the WAL log to recover the database state.
        - db_handler: Instance of DBHandler to apply changes to.
        """
        log_entries = self.read_log()
        for entry in log_entries:
            transaction_id, operation, *data = entry
            if operation == "F":  # Write operation
                data_id, old_value, new_value = map(int, data)
                db_handler.buffer[data_id] = new_value
                self.logger.info(f"Applied write log: {entry}")
            elif operation in ["S", "R", "C"]:
                # Start, rollback, or commit logs do not affect database directly
                self.logger.info(f"Processed {operation} log for transaction {transaction_id}: {entry}")

        # Ensure the recovered state is flushed to disk
        db_handler.write_database()
        self.logger.info("Database state recovered and flushed to disk.")

# Example usage
if __name__ == "__main__":
    from db_handler import DBHandler

    recovery_manager = RecoveryManager()
    db_handler = DBHandler()

    # Simulate some logs
    recovery_manager.write_log(1, "S")
    recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
    recovery_manager.write_log(1, "C")
    recovery_manager.write_log(2, "S")
    recovery_manager.write_log(2, "F", data_id=1, old_value=0, new_value=1)
    recovery_manager.write_log(2, "R")

    # Recover database state
    db_handler.read_database()
    recovery_manager.apply_logs(db_handler)
