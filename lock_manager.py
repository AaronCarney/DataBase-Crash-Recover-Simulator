from logging_config import get_logger

class LockManager:
    def __init__(self):
        """Initialize the LockManager and its data structures."""
        self.locks = {}  # Dictionary to track locks: {data_id: (lock_type, transaction_id)}
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("LockManager initialized.")

    def acquire_lock(self, transaction_id, data_id, lock_type):
        """
        Attempt to acquire a lock for a given transaction.
        - transaction_id: ID of the transaction requesting the lock.
        - data_id: ID of the data to lock.
        - lock_type: 'shared' (S) or 'exclusive' (X).
        """
        if data_id not in self.locks:
            self.locks[data_id] = (lock_type, transaction_id)
            self.logger.info(f"Transaction {transaction_id} acquired {lock_type} lock on {data_id}.")
            return True
        elif lock_type == "shared" and self.locks[data_id][0] == "shared":
            self.logger.info(f"Transaction {transaction_id} acquired shared lock on {data_id} (already shared).")
            return True
        else:
            self.logger.warning(f"Transaction {transaction_id} failed to acquire {lock_type} lock on {data_id}.")
            return False

    def release_locks(self, transaction_id):
        """
        Release all locks held by a transaction.
        - transaction_id: ID of the transaction releasing locks.
        """
        to_release = [data_id for data_id, (lock_type, tid) in self.locks.items() if tid == transaction_id]
        for data_id in to_release:
            del self.locks[data_id]
            self.logger.info(f"Transaction {transaction_id} released lock on {data_id}.")
        self.logger.debug(f"Transaction {transaction_id} locks released.")

    def check_deadlocks(self):
        """
        Check for deadlocks and resolve them if necessary.
        (Placeholder for now.)
        """
        self.logger.info("Checking for deadlocks.")
        # Add deadlock resolution logic later
        pass
