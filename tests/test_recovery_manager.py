import unittest
import os
from recovery_manager import RecoveryManager

class TestRecoveryManager(unittest.TestCase):
    def setUp(self):
        self.recovery_manager = RecoveryManager()
        # Ensure a clean slate for the log file
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
        self.recovery_manager.apply_logs()  # Should not raise an error

    def test_recovery_replay(self):
        self.recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
        self.recovery_manager.apply_logs()
        # Future integration test to verify replay of logs on DBHandler

if __name__ == "__main__":
    unittest.main()
