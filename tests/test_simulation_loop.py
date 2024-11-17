import os
import unittest
from db_handler import DBHandler
from lock_manager import LockManager
from main import simulation_loop
from recovery_manager import RecoveryManager
from transaction_manager import TransactionManager


class TestSimulationLoop(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.log_handler = None

    def setUp(self):
        # Ensure clean state for database and log files
        self.db_file = "test_db.txt"
        self.log_file = "test_log.csv"

        # Remove previous files to ensure a clean state
        for file in [self.db_file, self.log_file]:
            if os.path.exists(file):
                os.remove(file)

        # Initialize DBHandler
        self.db_handler = DBHandler(self.db_file)
        with open(self.db_file, "w") as f:
            f.write("0," * 31 + "0\n")  # Initialize database with 32 bits all set to 0

        # Initialize RecoveryManager with the DBHandler instance
        self.recovery_manager = RecoveryManager(self.db_handler)

        # Initialize LockManager
        self.lock_manager = LockManager()

        # Initialize TransactionManager
        self.transaction_manager = TransactionManager(self.lock_manager, self.recovery_manager, self.db_handler)

        # Redirect files for testing
        self.db_handler.db_file = self.db_file
        self.recovery_manager.log_file = self.log_file

    def test_basic_consistency(self):
        """
        Test basic consistency of database and logs after the simulation.
        This test ensures that the database file is created and transactions are correctly applied.
        """
        # Verify that the database handler exists
        self.assertIsNotNone(self.db_handler, "Database handler is not initialized.")

        # Ensure the database file does not exist at the start of the test
        if os.path.exists(self.db_handler.db_file):
            os.remove(self.db_handler.db_file)

        # Apply the logs to restore the database
        self.recovery_manager.apply_logs()  # Corrected reference here

        # Ensure that a database file is created
        self.db_handler.write_database()
        self.assertTrue(os.path.exists(self.db_handler.db_file), "Database file not created.")

        # Verify the content of the database
        with open(self.db_handler.db_file, "r") as f:
            database_content = f.read()
            self.assertIn("1", database_content, "Database does not contain expected updated values.")

    def test_recovery_after_crash(self):
        """Simulate a crash and verify recovery integrity."""
        # Create an incomplete simulation (simulate a crash)
        max_cycles = 30
        transaction_size = 5
        prob_start_transaction = 0.6
        prob_write = 0.7
        prob_rollback = 0.1

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # Simulate crash by re-initializing modules
        self.db_handler = DBHandler(self.db_file)
        self.recovery_manager = RecoveryManager(self.log_file)
        self.db_handler.read_database()
        self.recovery_manager.apply_logs()

        # Validate recovered database matches expected state
        with open(self.db_file, "r") as db_file:
            db_content = list(map(int, db_file.readline().strip().split(",")))

        log_entries = self.recovery_manager.read_log()
        recovered_buffer = [0] * 32
        for entry in log_entries:
            if entry[1] == "F":  # Write operation
                _, _, data_id, _, new_value = map(int, entry)
                recovered_buffer[data_id] = new_value

        self.assertEqual(db_content, recovered_buffer, "Database and logs do not match after recovery.")

    def tearDown(self):
        # Clean up test files
        for file in [self.db_file, self.log_file]:
            if os.path.exists(file):
                os.remove(file)

    def test_edge_probabilities(self):
        """Test the simulation with edge-case probabilities."""
        # Scenario 1: All writes (rollback_prob = 0)
        max_cycles = 30
        transaction_size = 10
        prob_start_transaction = 0.8
        prob_write = 1.0  # Always write
        prob_rollback = 0.0  # No rollbacks

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # Validate database consistency
        with open(self.db_file, "r") as db_file:
            db_content = list(map(int, db_file.readline().strip().split(",")))

        log_entries = self.recovery_manager.read_log()
        recovered_buffer = [0] * 32
        for entry in log_entries:
            if entry[1] == "F":  # Check operation type
                _, _, data_id, _, new_value = map(int, entry[2:])  # Skip operation field
                recovered_buffer[data_id] = new_value

        self.assertEqual(db_content, recovered_buffer, "Database and logs do not match for all-write scenario.")

        # Scenario 2: All rollbacks (rollback_prob = 1)
        self.setUp()  # Reinitialize for a fresh run
        prob_write = 0.0
        prob_rollback = 1.0  # Always rollback

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # Ensure no database changes occurred
        with open(self.db_file, "r") as db_file:
            db_content_after = list(map(int, db_file.readline().strip().split(",")))
        self.assertEqual(db_content_after, [0] * 32, "Database should remain unchanged when all operations rollback.")

    def test_high_contention(self):
        """Test system under high contention."""
        max_cycles = 50
        transaction_size = 5
        prob_start_transaction = 1.0  # Always start a transaction each cycle
        prob_write = 0.7
        prob_rollback = 0.2

        simulation_loop(
            self.db_handler, self.recovery_manager, self.lock_manager, self.transaction_manager,
            max_cycles, transaction_size, prob_start_transaction, prob_write, prob_rollback
        )

        # Validate no lingering locks
        self.assertEqual(len(self.lock_manager.locks), 0, "All locks should be released after simulation.")

        # Validate fairness (no transaction monopolizes resources)
        lock_distribution = [len(trans) for trans in self.lock_manager.locked_data_by_transaction.values()]
        self.assertTrue(all(count <= 1 for count in lock_distribution), "Locks should be fairly distributed.")


if __name__ == "__main__":
    unittest.main()
