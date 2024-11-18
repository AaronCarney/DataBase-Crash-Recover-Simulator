import unittest
import os
from db_handler import DBHandler
from recovery_manager import RecoveryManager


class TestRecoveryManager(unittest.TestCase):
    def setUp(self):
        self.db_handler = DBHandler()
        self.recovery_manager = RecoveryManager(self.db_handler)
        if os.path.exists(self.recovery_manager.log_file):
            os.remove(self.recovery_manager.log_file)

    def test_write_log(self):
        self.recovery_manager.write_log(1, "S")
        self.recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
        with open(self.recovery_manager.log_file, "r") as f:
            logs = f.readlines()
        self.assertEqual(logs[0].strip(), "1,S")
        self.assertEqual(logs[1].strip(), "1,F,0,0,1")

    def test_log_file_creation(self):
        self.recovery_manager.write_log(1, "S")
        self.assertTrue(os.path.exists(self.recovery_manager.log_file))

    def test_missing_log_file(self):
        if os.path.exists(self.recovery_manager.log_file):
            os.remove(self.recovery_manager.log_file)
        self.recovery_manager.apply_logs()  # Ensure no exceptions are raised

    def test_recovery_replay(self):
        self.recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
        self.recovery_manager.apply_logs()  # Apply logs to simulate recovery
        self.assertEqual(self.db_handler.buffer[0], 1)

    def test_replay_logs_after_crash(self):
        """
        Verify database state consistency after applying recovery logs.
        """
        self.db_handler = DBHandler()  # Initialize the DBHandler instance for use in this test
        self.recovery_manager = RecoveryManager(self.db_handler)
        self.recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
        self.recovery_manager.write_log(2, "F", data_id=1, old_value=0, new_value=1)
        self.db_handler.read_database()
        self.recovery_manager.apply_logs()
        self.assertEqual(self.db_handler.buffer[0], 1, "Log replay did not update buffer[0].")
        self.assertEqual(self.db_handler.buffer[1], 1, "Log replay did not update buffer[1].")

    def test_apply_logs_with_valid_data(self):
        self.recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
        self.recovery_manager.apply_logs()
        self.assertEqual(self.db_handler.buffer[0], 1)


if __name__ == "__main__":
    unittest.main()
