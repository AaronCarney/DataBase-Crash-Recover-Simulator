import unittest
import os
from db_handler import DBHandler
from recovery_manager import RecoveryManager


class TestRecoveryManager(unittest.TestCase):
    def setUp(self):
        self.db_handler = DBHandler(db_file="test_db")
        self.recovery_manager = RecoveryManager(self.db_handler, log_file="test_log")
        if os.path.exists(self.recovery_manager.log_file):
            os.remove(self.recovery_manager.log_file)

    def test_write_log(self):
        self.recovery_manager.write_log(1, operation="S")
        self.recovery_manager.write_log(1, data_id=0, old_value=0, operation="F")
        with open(self.recovery_manager.log_file, "r") as f:
            logs = f.readlines()
        self.assertEqual(logs[0].strip(), "1,S")
        self.assertEqual(logs[1].strip(), "1,0,0,F")

    def test_log_file_creation(self):
        self.recovery_manager.write_log(1, operation="S")
        self.assertTrue(os.path.exists(self.recovery_manager.log_file))

    def test_missing_log_file(self):
        if os.path.exists(self.recovery_manager.log_file):
            os.remove(self.recovery_manager.log_file)
        self.recovery_manager.apply_logs()  # Ensure no exceptions are raised

    def test_recovery_replay(self):
        self.recovery_manager.write_log(1, data_id=0, old_value=0, operation="F")
        self.recovery_manager.write_log(1, operation="C")  # Add commit entry
        self.recovery_manager.apply_logs()  # Apply logs to simulate recovery
        self.assertEqual(self.db_handler.buffer[0], 1)

    def test_apply_logs_with_valid_data(self):
        self.recovery_manager.write_log(1, data_id=0, old_value=0, operation="F")
        self.recovery_manager.write_log(1, operation="C")  # Add commit entry
        self.recovery_manager.apply_logs()
        self.assertEqual(self.db_handler.buffer[0], 1)


if __name__ == "__main__":
    unittest.main()
