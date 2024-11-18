import os
import unittest
from db_handler import DBHandler
from lock_manager import LockManager
from logging_config import setup_logging
from main import simulation_loop
from recovery_manager import RecoveryManager
from transaction_manager import TransactionManager


class TestSimulationLoop(unittest.TestCase):
    def setUp(self):
        # Ensure clean state for database and log files
        self.db_file = "test_db"
        self.log_file = "test_log"

        # Remove previous files to ensure a clean state
        for file in [self.db_file, self.log_file]:
            if os.path.exists(file):
                os.remove(file)

        # Initialize logging
        setup_logging(log_file="test_adbsim.log")

        # Initialize DBHandler
        self.db_handler = DBHandler(db_file=self.db_file)
        self.db_handler.read_database()

        # Initialize RecoveryManager with the DBHandler instance
        self.recovery_manager = RecoveryManager(self.db_handler, log_file=self.log_file)
        self.recovery_manager.apply_logs()

        # Initialize LockManager with a timeout of 5 cycles
        self.lock_manager = LockManager(timeout_cycles=5)

        # Initialize TransactionManager
        self.transaction_manager = TransactionManager(self.lock_manager, self.recovery_manager, self.db_handler)

    def test_simulation_with_parameters(self):
        """
        Test the simulation loop with predefined parameters to ensure it runs without errors.
        """
        max_cycles = 10
        transaction_size = 3
        prob_start_transaction = 0.7  # 70% chance to start a transaction each cycle
        prob_write = 0.5              # 50% chance of a write operation
        prob_rollback = 0.2           # 20% chance of a rollback

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # Since the simulation prints the final database state, we can check the buffer directly
        self.assertEqual(len(self.db_handler.buffer), 32, "Database buffer should have 32 elements.")

    def test_simulation_no_transactions(self):
        """
        Test the simulation loop with zero probability of starting transactions.
        """
        max_cycles = 5
        transaction_size = 3
        prob_start_transaction = 0.0  # No transactions will start
        prob_write = 0.5
        prob_rollback = 0.2

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # The database should remain unchanged
        self.assertEqual(self.db_handler.buffer, [0] * 32, "Database should remain unchanged when no transactions "
                                                           "start.")

    def test_simulation_all_rollbacks(self):
        """
        Test the simulation where all transactions rollback.
        """
        max_cycles = 10
        transaction_size = 3
        prob_start_transaction = 1.0   # Always start a transaction
        prob_write = 0.0               # No write operations
        prob_rollback = 1.0            # Always rollback

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # The database should remain unchanged
        self.assertEqual(self.db_handler.buffer, [0] * 32, "Database should remain unchanged when all transactions "
                                                           "rollback.")

    def test_simulation_all_commits(self):
        """
        Test the simulation where all transactions commit successfully.
        """
        max_cycles = 10
        transaction_size = 3
        prob_start_transaction = 1.0   # Always start a transaction
        prob_write = 1.0               # Always write
        prob_rollback = 0.0            # No rollbacks

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # The database should have changes
        self.assertNotEqual(self.db_handler.buffer, [0] * 32, "Database should have been updated by committed "
                                                              "transactions.")

    def tearDown(self):
        # Clean up test files
        for file in [self.db_file, self.log_file, "test_adbsim.log"]:
            if os.path.exists(file):
                os.remove(file)


if __name__ == "__main__":
    unittest.main()
