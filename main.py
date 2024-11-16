from lock_manager import LockManager
from logging_config import setup_logging, get_logger

if __name__ == "__main__":
    setup_logging()
    logger = get_logger("main")
    logger.info("Logging is set up and working correctly.")

    lock_manager = LockManager()
    lock_manager.acquire_lock(1, "data1", "shared")
    lock_manager.acquire_lock(2, "data1", "exclusive")
    lock_manager.release_locks(1)
    lock_manager.check_deadlocks()
