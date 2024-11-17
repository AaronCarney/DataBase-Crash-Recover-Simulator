import unittest
from lock_manager import LockManager


class TestLockManager(unittest.TestCase):
    def setUp(self):
        self.lock_manager = LockManager()

    def test_acquire_lock(self):
        self.assertTrue(self.lock_manager.acquire_lock(1, "data1", "shared"))
        self.assertFalse(self.lock_manager.acquire_lock(2, "data1", "exclusive"))

    def test_multiple_shared_locks(self):
        self.assertTrue(self.lock_manager.acquire_lock(1, "data1", "shared"))
        self.assertTrue(self.lock_manager.acquire_lock(2, "data1", "shared"))

    def test_exclusive_lock_after_shared(self):
        self.lock_manager.acquire_lock(1, "data1", "shared")
        self.lock_manager.acquire_lock(2, "data1", "shared")
        self.assertFalse(self.lock_manager.acquire_lock(3, "data1", "exclusive"))

    def test_lock_release_and_reacquire(self):
        self.lock_manager.acquire_lock(1, "data1", "exclusive")
        self.lock_manager.release_locks(1)
        self.assertTrue(self.lock_manager.acquire_lock(2, "data1", "exclusive"))

    def test_deadlock_detection_logging(self):
        self.lock_manager.check_deadlocks()  # Ensure it doesn't crash or raise exceptions

    def test_deadlock_resolution(self):
        """
        Verify that deadlocks are resolved by timing out the blocked transactions.
        """
        self.lock_manager.acquire_lock(1, "data1", "exclusive")
        self.lock_manager.acquire_lock(2, "data2", "exclusive")
        # Simulate a deadlock
        self.lock_manager.lock_queue["data1"].append((2, "shared"))
        self.lock_manager.lock_queue["data2"].append((1, "shared"))
        self.lock_manager.transaction_timestamps[1] -= 10  # Simulate timeout for transaction 1

        self.lock_manager.check_deadlocks()

        # Ensure transaction 1 is aborted and its locks are released
        self.assertNotIn(1, self.lock_manager.transaction_timestamps)
        self.assertNotIn("data1", self.lock_manager.locks)


if __name__ == "__main__":
    unittest.main()
