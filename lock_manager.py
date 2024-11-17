from logging_config import get_logger
from collections import defaultdict
import time


class LockManager:
    def __init__(self):
        """Initialize the LockManager and its data structures."""
        self.locks = {}  # Dictionary to track locks: {data_id: (lock_type, transaction_ids)}
        self.lock_queue = defaultdict(list)  # Queue for pending lock requests: {data_id: [(transaction_id, lock_type)]}
        self.transaction_timestamps = {}  # Track when transactions started (for timeout-based deadlock resolution)
        self.locked_data_by_transaction = defaultdict(set)  # Track locked data per transaction
        self.logger = get_logger(self.__class__.__name__)
        self.deadlock_timeout = 5  # Time in seconds to wait before resolving deadlocks
        self.logger.info("LockManager initialized.")

    def acquire_lock(self, transaction_id, data_id, lock_type):
        """
        Attempt to acquire a lock for a given transaction.
        - transaction_id: ID of the transaction requesting the lock.
        - data_id: ID of the data to lock.
        - lock_type: 'shared' (S) or 'exclusive' (X).
        """
        if transaction_id not in self.transaction_timestamps:
            self.transaction_timestamps[transaction_id] = time.time()

        # Check if lock can be granted immediately
        if data_id not in self.locks:
            self.locks[data_id] = (lock_type, {transaction_id})
            self.locked_data_by_transaction[transaction_id].add(data_id)
            self.logger.info(f"Transaction {transaction_id} acquired {lock_type} lock on {data_id}.")
            return True

        current_lock_type, current_transactions = self.locks[data_id]
        if lock_type == "shared" and current_lock_type == "shared":
            # Allow shared locks to be acquired simultaneously
            current_transactions.add(transaction_id)
            self.locked_data_by_transaction[transaction_id].add(data_id)
            self.logger.info(f"Transaction {transaction_id} acquired shared lock on {data_id}.")
            return True
        elif lock_type == "exclusive" and current_transactions == {transaction_id}:
            # Escalate lock for the same transaction
            self.locks[data_id] = ("exclusive", current_transactions)
            self.logger.info(f"Transaction {transaction_id} escalated lock to exclusive on {data_id}.")
            return True

        # Otherwise, add to the queue
        self.lock_queue[data_id].append((transaction_id, lock_type))
        self.logger.warning(f"Transaction {transaction_id} is waiting for {lock_type} lock on {data_id}.")
        return False

    def release_locks(self, transaction_id):
        """
        Release all locks held by a transaction.
        - transaction_id: ID of the transaction releasing locks.
        """
        if transaction_id not in self.locked_data_by_transaction:
            self.logger.warning(f"Transaction {transaction_id} has no locks to release.")
            return

        for data_id in self.locked_data_by_transaction[transaction_id]:
            lock_type, current_transactions = self.locks[data_id]
            current_transactions.remove(transaction_id)
            if not current_transactions:
                # If no transactions are holding the lock, clear it
                del self.locks[data_id]
                self.logger.info(f"Lock on {data_id} has been released.")

                # Clear all pending requests for this data_id
                if data_id in self.lock_queue:
                    self.logger.info(f"Clearing lock queue for {data_id} due to lock release.")
                    del self.lock_queue[data_id]

        del self.locked_data_by_transaction[transaction_id]
        del self.transaction_timestamps[transaction_id]
        self.logger.info(f"Transaction {transaction_id} released all locks.")

    def check_deadlocks(self):
        """
        Check for deadlocks using timeout mechanism.
        Transactions waiting for too long are aborted.
        """
        current_time = time.time()
        for transaction_id, start_time in list(self.transaction_timestamps.items()):
            if current_time - start_time > self.deadlock_timeout:
                self.logger.warning(f"Transaction {transaction_id} has timed out and is being aborted.")
                self.release_locks(transaction_id)

        self.logger.info("Deadlock check completed.")


# Example usage
if __name__ == "__main__":
    lock_manager = LockManager()
    lock_manager.acquire_lock(1, "data1", "shared")
    lock_manager.acquire_lock(2, "data1", "exclusive")
    lock_manager.release_locks(1)
    lock_manager.check_deadlocks()
