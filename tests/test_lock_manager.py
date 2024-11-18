import unittest
from lock_manager import LockManager


class TestLockManager(unittest.TestCase):
    def setUp(self):
        self.lock_manager = LockManager(timeout_cycles=5)

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

    def test_deadlock_detection(self):
        """
        Verify that deadlocks are resolved by timing out the blocked transactions.
        """
        self.lock_manager.acquire_lock(1, "data1", "exclusive")
        self.lock_manager.acquire_lock(2, "data2", "exclusive")
        # Simulate a deadlock
        self.lock_manager.lock_queue["data1"].append((2, "exclusive"))
        self.lock_manager.lock_queue["data2"].append((1, "exclusive"))
        # Simulate cycles
        for _ in range(6):
            self.lock_manager.increment_cycle()
        self.lock_manager.check_deadlocks()
        self.assertNotIn(1, self.lock_manager.transaction_wait_cycles)
        self.assertNotIn(2, self.lock_manager.transaction_wait_cycles)


if __name__ == "__main__":
    unittest.main()
