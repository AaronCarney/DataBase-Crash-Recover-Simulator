from logging_config import get_logger


class TransactionManager:
    def __init__(self, lock_manager, recovery_manager, db_handler):
        self.lock_manager = lock_manager
        self.recovery_manager = recovery_manager
        self.db_handler = db_handler
        self.transactions = {}
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("TransactionManager initialized.")

    def start_transaction(self, transaction_id):
        """Start a new transaction."""
        if transaction_id in self.transactions:
            self.logger.warning(f"Transaction {transaction_id} already exists.")
            return False
        self.transactions[transaction_id] = {"state": "active", "operations": [], "blocked": False}
        self.recovery_manager.write_log(transaction_id, operation="S")
        self.logger.info(f"Transaction {transaction_id} started.")
        return True

    def submit_operation(self, transaction_id, data_id, operation):
        """
        Submit an operation for a transaction.
        - operation: 'F' for write.
        """
        if transaction_id not in self.transactions:
            self.logger.warning(f"Transaction {transaction_id} not found.")
            return False

        if self.transactions[transaction_id]["state"] != "active":
            self.logger.warning(f"Transaction {transaction_id} is not active.")
            return False

        if self.transactions[transaction_id]["blocked"]:
            self.logger.info(f"Transaction {transaction_id} is blocked.")
            return False

        # Attempt to acquire a lock
        lock_type = "exclusive" if operation == "F" else "shared"
        if not self.lock_manager.acquire_lock(transaction_id, data_id, lock_type):
            self.logger.info(f"Transaction {transaction_id} is blocked waiting for lock on {data_id}.")
            self.transactions[transaction_id]["blocked"] = True
            return False

        # Log and execute the operation
        if operation == "F":
            old_value = self.db_handler.buffer[data_id]
            # Toggle the value for simplicity
            new_value = 1 if old_value == 0 else 0
            self.db_handler.buffer[data_id] = new_value
            self.recovery_manager.write_log(transaction_id, data_id=data_id, old_value=old_value, operation="F")
            self.logger.info(f"Transaction {transaction_id} performed write on {data_id}: {old_value} -> {new_value}.")

            # Record operation in transaction
            self.transactions[transaction_id]["operations"].append((data_id, operation, old_value, new_value))

        return True

    def rollback_transaction(self, transaction_id):
        """Rollback a transaction."""
        if transaction_id not in self.transactions or self.transactions[transaction_id]["state"] != "active":
            self.logger.warning(f"Cannot rollback transaction {transaction_id}.")
            return False

        # Revert changes made by the transaction
        for data_id, operation, old_value, new_value in reversed(self.transactions[transaction_id]["operations"]):
            if operation == "F":
                self.db_handler.buffer[data_id] = old_value
                self.logger.info(f"Rolled back write on {data_id}: {new_value} -> {old_value}.")

        self.transactions[transaction_id]["state"] = "rolled_back"
        self.recovery_manager.write_log(transaction_id, operation="R")
        self.lock_manager.release_locks(transaction_id)
        self.logger.info(f"Transaction {transaction_id} rolled back.")
        return True

    def commit_transaction(self, transaction_id):
        """Commit a transaction."""
        if transaction_id not in self.transactions or self.transactions[transaction_id]["state"] != "active":
            self.logger.warning(f"Cannot commit transaction {transaction_id}.")
            return False

        self.transactions[transaction_id]["state"] = "committed"
        self.recovery_manager.write_log(transaction_id, operation="C")
        self.lock_manager.release_locks(transaction_id)
        self.logger.info(f"Transaction {transaction_id} committed.")
        return True

    def unblock_transactions(self):
        """
        Attempt to unblock transactions if their locks can now be acquired.
        """
        for transaction_id in self.transactions:
            if self.transactions[transaction_id]["blocked"] and self.transactions[transaction_id]["state"] == "active":
                pending_ops = self.transactions[transaction_id]["operations"]
                if pending_ops:
                    last_op = pending_ops[-1]
                    data_id = last_op[0]
                    lock_type = "exclusive" if last_op[1] == "F" else "shared"
                    if self.lock_manager.acquire_lock(transaction_id, data_id, lock_type):
                        self.transactions[transaction_id]["blocked"] = False
                        self.logger.info(f"Transaction {transaction_id} is unblocked.")
