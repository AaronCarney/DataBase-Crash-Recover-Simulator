from logging_config import get_logger
from collections import defaultdict


class LockManager:
    def __init__(self, timeout_cycles):
        """Initialize the LockManager and its data structures."""
        self.locks = {}  # {data_id: (lock_type, set of transaction_ids)}
        self.lock_queue = defaultdict(list)  # {data_id: [(transaction_id, lock_type)]}
        self.transaction_wait_cycles = {}  # {transaction_id: cycles_waited}
        self.transaction_lock_time = {}  # {transaction_id: cycles since lock request}
        self.locked_data_by_transaction = defaultdict(set)  # {transaction_id: set of data_ids}
        self.logger = get_logger(self.__class__.__name__)
        self.deadlock_timeout = timeout_cycles  # Timeout in cycles
        self.current_cycle = 0  # Keep track of simulation cycles
        self.logger.info("LockManager initialized with timeout of {} cycles.".format(timeout_cycles))

    def acquire_lock(self, transaction_id, data_id, lock_type):
        """
        Attempt to acquire a lock for a given transaction.
        Returns True if the lock is acquired, False otherwise.
        """
        # Initialize wait cycles if not already set
        if transaction_id not in self.transaction_wait_cycles:
            self.transaction_wait_cycles[transaction_id] = 0

        # If data_id is not locked
        if data_id not in self.locks:
            self.locks[data_id] = (lock_type, {transaction_id})
            self.locked_data_by_transaction[transaction_id].add(data_id)
            self.logger.info(f"Transaction {transaction_id} acquired {lock_type} lock on {data_id}.")
            return True

        current_lock_type, current_transactions = self.locks[data_id]

        # Check if lock can be shared
        if lock_type == "shared" and current_lock_type == "shared":
            self.locks[data_id][1].add(transaction_id)
            self.locked_data_by_transaction[transaction_id].add(data_id)
            self.logger.info(f"Transaction {transaction_id} acquired shared lock on {data_id}.")
            return True

        # Check if the transaction already holds the lock
        if transaction_id in current_transactions:
            if current_lock_type == "shared" and lock_type == "exclusive":
                # Upgrade lock
                if len(current_transactions) == 1:
                    self.locks[data_id] = (lock_type, current_transactions)
                    self.logger.info(f"Transaction {transaction_id} upgraded to exclusive lock on {data_id}.")
                    return True
                else:
                    self.logger.info(f"Transaction {transaction_id} cannot upgrade to exclusive lock on {data_id} "
                                     f"because other transactions hold the lock.")
            else:
                # Lock already held
                return True

        # Otherwise, add to the queue
        self.lock_queue[data_id].append((transaction_id, lock_type))
        self.logger.warning(f"Transaction {transaction_id} is waiting for {lock_type} lock on {data_id}.")
        return False

    def release_locks(self, transaction_id):
        """
        Release all locks held by a transaction.
        """
        if transaction_id not in self.locked_data_by_transaction:
            self.logger.warning(f"Transaction {transaction_id} has no locks to release.")
            return

        for data_id in self.locked_data_by_transaction[transaction_id]:
            lock_type, current_transactions = self.locks[data_id]
            current_transactions.remove(transaction_id)
            if not current_transactions:
                # No more transactions holding the lock
                del self.locks[data_id]
                self.logger.info(f"Lock on {data_id} has been released.")

                # Try to grant locks to waiting transactions
                if data_id in self.lock_queue and self.lock_queue[data_id]:
                    self.logger.info(f"Attempting to grant locks to waiting transactions on {data_id}.")
                    self._grant_locks(data_id)
        del self.locked_data_by_transaction[transaction_id]
        if transaction_id in self.transaction_wait_cycles:
            del self.transaction_wait_cycles[transaction_id]
        if transaction_id in self.transaction_lock_time:
            del self.transaction_lock_time[transaction_id]
        self.logger.info(f"Transaction {transaction_id} released all locks.")

    def _grant_locks(self, data_id):
        """
        Grant locks to waiting transactions if possible.
        """
        while self.lock_queue[data_id]:
            waiting_transaction_id, requested_lock_type = self.lock_queue[data_id][0]
            can_grant = False

            if requested_lock_type == "shared":
                # Check if any waiting exclusive lock is ahead
                exclusive_waiting = any(
                    req_lock == "exclusive" for _, req_lock in self.lock_queue[data_id]
                )
                if not exclusive_waiting:
                    can_grant = True
            else:
                # Exclusive lock can only be granted if no other locks are held
                can_grant = True

            if can_grant:
                self.lock_queue[data_id].pop(0)
                self.acquire_lock(waiting_transaction_id, data_id, requested_lock_type)
                self.logger.info(f"Granted {requested_lock_type} lock on {data_id} "
                                 f"to transaction {waiting_transaction_id}.")
            else:
                break

    def increment_cycle(self):
        """
        Increment the cycle counter for deadlock detection.
        """
        self.current_cycle += 1
        # Increment wait cycles for transactions in lock queues
        for data_id, waiting_list in self.lock_queue.items():
            for transaction_id, _ in waiting_list:
                self.transaction_wait_cycles[transaction_id] += 1

    def check_deadlocks(self):
        """
        Check for deadlocks and abort transactions that have been waiting too long.
        """
        aborted_transactions = []
        for transaction_id, wait_cycles in self.transaction_wait_cycles.items():
            if wait_cycles >= self.deadlock_timeout:
                self.logger.warning(f"Transaction {transaction_id} aborted due to deadlock (waited {wait_cycles} "
                                    f"cycles).")
                aborted_transactions.append(transaction_id)

        for transaction_id in aborted_transactions:
            self.release_locks(transaction_id)
            # Remove transaction from lock queues
            for data_id, waiting_list in self.lock_queue.items():
                self.lock_queue[data_id] = [
                    (tid, ltype) for tid, ltype in waiting_list if tid != transaction_id
                ]
            if transaction_id in self.transaction_wait_cycles:
                del self.transaction_wait_cycles[transaction_id]
            if transaction_id in self.transaction_lock_time:
                del self.transaction_lock_time[transaction_id]
        if aborted_transactions:
            self.logger.info(f"Deadlock resolution: aborted transactions {aborted_transactions}")