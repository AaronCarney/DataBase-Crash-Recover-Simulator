from logging_config import get_logger

class TransactionManager:
    def __init__(self, lock_manager, recovery_manager):
        """Initialize the TransactionManager with required components."""
        self.lock_manager = lock_manager
        self.recovery_manager = recovery_manager
        self.transactions = {}  # {transaction_id: {"state": "active", "operations": []}}
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("TransactionManager initialized.")

    def start_transaction(self, transaction_id):
        """Start a new transaction."""
        self.transactions[transaction_id] = {"state": "active", "operations": []}
        self.recovery_manager.write_log(transaction_id, "S")
        self.logger.info(f"Transaction {transaction_id} started.")

    def submit_operation(self, transaction_id, data_id, operation, old_value=None, new_value=None):
        """
        Submit an operation for a transaction.
        - operation: 'F' for write.
        """
        if transaction_id not in self.transactions or self.transactions[transaction_id]["state"] != "active":
            self.logger.warning(f"Cannot submit operation for inactive transaction {transaction_id}.")
            return False

        # Add operation to the transaction
        self.transactions[transaction_id]["operations"].append((data_id, operation, old_value, new_value))
        if operation == "F":
            self.recovery_manager.write_log(transaction_id, "F", data_id, old_value, new_value)
        self.logger.info(f"Transaction {transaction_id}: Operation {operation} on {data_id} recorded.")
        return True

    def rollback_transaction(self, transaction_id):
        """Rollback a transaction."""
        if transaction_id not in self.transactions:
            self.logger.warning(f"Transaction {transaction_id} not found for rollback.")
            return False

        self.transactions[transaction_id]["state"] = "rolled_back"
        self.recovery_manager.write_log(transaction_id, "R")
        self.lock_manager.release_locks(transaction_id)
        self.logger.info(f"Transaction {transaction_id} rolled back.")
        return True

    def commit_transaction(self, transaction_id):
        """Commit a transaction."""
        if transaction_id not in self.transactions:
            self.logger.warning(f"Transaction {transaction_id} not found for commit.")
            return False

        self.transactions[transaction_id]["state"] = "committed"
        self.recovery_manager.write_log(transaction_id, "C")
        self.lock_manager.release_locks(transaction_id)
        self.logger.info(f"Transaction {transaction_id} committed.")
        return True
