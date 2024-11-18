from logging_config import get_logger
import os


class RecoveryManager:
    def __init__(self, db_handler, log_file="log"):
        """Initialize the RecoveryManager."""
        self.db_handler = db_handler
        self.logger = get_logger(self.__class__.__name__)
        self.log_file = log_file
        self.write_count = 0  # Track the number of writes since the last flush
        self.logger.info("RecoveryManager initialized.")

    def write_log(self, transaction_id, data_id=None, old_value=None, operation=None):
        """
        Write an operation to the WAL log.
        - transaction_id: ID of the transaction performing the operation.
        - data_id: ID of the data involved (if applicable).
        - old_value: The old value of the data (if applicable).
        - operation: The type of operation ('S', 'F', 'R', 'C').
        """
        log_entry = [str(transaction_id)]
        if data_id is not None:
            log_entry.extend([str(data_id), str(old_value)])
        if operation is not None:
            log_entry.append(operation)

        # Write to the log file
        with open(self.log_file, "a") as f:
            f.write(",".join(log_entry) + "\n")

        self.logger.info(f"Log entry added: {log_entry}")
        self.write_count += 1

        # Flush logs every 25 write operations
        if self.write_count >= 25:
            self.flush_logs()

    def flush_logs(self):
        """
        Flush log entries to ensure durability.
        (In this implementation, logs are flushed after every write.)
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
        with open(self.log_file, "r") as f:
            for line in f:
                parts = line.strip().split(",")
                log_entries.append(parts)
        self.logger.info(f"Read {len(log_entries)} log entries from the log file.")
        return log_entries

    def apply_logs(self):
        if not os.path.exists(self.log_file):
            self.logger.warning(f"Log file {self.log_file} does not exist. No logs to apply.")
            return

        log_entries = self.read_log()

        # Determine committed transactions
        committed_transactions = set()
        for parts in log_entries:
            if len(parts) >= 2 and parts[-1] == "C":
                transaction_id = int(parts[0])
                committed_transactions.add(transaction_id)

        # Apply 'F' entries only for committed transactions
        for parts in log_entries:
            if len(parts) >= 4 and parts[-1] == "F":
                transaction_id = int(parts[0])
                if transaction_id in committed_transactions:
                    data_id = int(parts[1])
                    old_value = int(parts[2])
                    # Toggle the value since new_value is not stored in the log
                    new_value = 1 if old_value == 0 else 0
                    self.db_handler.update_buffer(data_id, new_value)
                    self.logger.info(
                        f"Transaction {transaction_id}: Applied 'F' log entry on data_id {data_id}: {old_value} "
                        f"-> {new_value}")
                else:
                    self.logger.info(
                        f"Transaction {transaction_id}: 'F' log entry skipped (transaction not committed).")

        self.db_handler.write_database()
        self.logger.info("Database state recovered and flushed to disk.")
