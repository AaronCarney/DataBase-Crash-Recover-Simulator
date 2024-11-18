import csv
from logging_config import get_logger
import os


class RecoveryManager:
    def __init__(self, db_handler, log_file="log.csv"):
        """Initialize the RecoveryManager."""
        self.db_handler = db_handler
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

    def apply_logs(self):
        if not os.path.exists(self.log_file):
            self.logger.warning(f"Log file {self.log_file} does not exist. No logs to apply.")
            return

        # Read all log entries
        log_entries = []
        with open(self.log_file, "r") as f:
            for line in f:
                parts = line.strip().split(",")
                log_entries.append(parts)

        # Determine committed transactions
        committed_transactions = set()
        for parts in log_entries:
            if len(parts) < 2:
                continue
            transaction_id = int(parts[0])
            operation = parts[1]
            if operation == "C":
                committed_transactions.add(transaction_id)

        # Apply 'F' entries only for committed transactions
        for parts in log_entries:
            if len(parts) >= 5 and parts[1] == "F":
                transaction_id = int(parts[0])
                if transaction_id in committed_transactions:
                    data_id = int(parts[2])
                    old_value = int(parts[3])
                    new_value = int(parts[4])
                    self.db_handler.update_buffer(data_id, new_value)
                    self.logger.info(
                        f"Transaction {transaction_id}: Applied 'F' log entry on data_id {data_id}: {old_value} "
                        f"-> {new_value}")
                else:
                    self.logger.info(
                        f"Transaction {transaction_id}: 'F' log entry skipped (transaction not committed).")

        self.db_handler.write_database()
        self.logger.info("Database state recovered and flushed to disk.")