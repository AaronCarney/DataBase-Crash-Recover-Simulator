import unittest

from db_handler import DBHandler
from transaction_manager import TransactionManager
from lock_manager import LockManager
from recovery_manager import RecoveryManager


class TestTransactionManager(unittest.TestCase):
    def setUp(self):
        self.lock_manager = LockManager(timeout_cycles=5)
        self.db_handler = DBHandler()
        self.recovery_manager = RecoveryManager(self.db_handler)
        self.transaction_manager = TransactionManager(self.lock_manager, self.recovery_manager, self.db_handler)

    def test_start_transaction(self):
        self.transaction_manager.start_transaction(1)
        self.assertIn(1, self.transaction_manager.transactions)
        self.assertEqual(self.transaction_manager.transactions[1]["state"], "active")

    def test_commit_transaction(self):
        self.transaction_manager.start_transaction(1)
        self.transaction_manager.commit_transaction(1)
        self.assertEqual(self.transaction_manager.transactions[1]["state"], "committed")

    def test_rollback_transaction(self):
        self.transaction_manager.start_transaction(1)
        self.transaction_manager.rollback_transaction(1)
        self.assertEqual(self.transaction_manager.transactions[1]["state"], "rolled_back")

    def test_submit_operation_inactive_transaction(self):
        self.transaction_manager.start_transaction(1)
        self.transaction_manager.rollback_transaction(1)
        result = self.transaction_manager.submit_operation(1, 0, "F")
        self.assertFalse(result)

    def test_transaction_states(self):
        self.transaction_manager.start_transaction(1)
        self.assertEqual(self.transaction_manager.transactions[1]["state"], "active")
        self.transaction_manager.commit_transaction(1)
        self.assertEqual(self.transaction_manager.transactions[1]["state"], "committed")

    def test_concurrent_transactions(self):
        self.transaction_manager.start_transaction(1)
        self.transaction_manager.start_transaction(2)
        self.transaction_manager.submit_operation(1, 0, "F")
        self.transaction_manager.submit_operation(2, 1, "F")
        self.assertEqual(len(self.transaction_manager.transactions[1]["operations"]), 1)
        self.assertEqual(len(self.transaction_manager.transactions[2]["operations"]), 1)

    def test_blocked_transaction(self):
        """
        Test that a transaction is correctly marked as blocked when resources are unavailable.
        """
        self.transaction_manager.start_transaction(1)
        self.transaction_manager.submit_operation(1, 0, "F")
        self.transaction_manager.start_transaction(2)
        result = self.transaction_manager.submit_operation(2, 0, "F")  # Transaction 2 should be blocked
        self.assertFalse(result)
        self.assertTrue(self.transaction_manager.transactions[2]["blocked"])


if __name__ == "__main__":
    unittest.main()
