from db_handler import DBHandler
from lock_manager import LockManager
from logging_config import setup_logging, get_logger
from recovery_manager import RecoveryManager
from transaction_manager import TransactionManager

if __name__ == "__main__":
    setup_logging()
    logger = get_logger("main")
    logger.info("Logging is set up and working correctly.")

    lock_manager = LockManager()
    lock_manager.acquire_lock(1, "data1", "shared")
    lock_manager.acquire_lock(2, "data1", "exclusive")
    lock_manager.release_locks(1)
    lock_manager.check_deadlocks()

    recovery_manager = RecoveryManager()
    recovery_manager.write_log(1, "S")
    recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
    recovery_manager.apply_logs()

    transaction_manager = TransactionManager(lock_manager, recovery_manager)
    transaction_manager.start_transaction(1)
    transaction_manager.submit_operation(1, "data1", "F", old_value=0, new_value=1)
    transaction_manager.commit_transaction(1)
    transaction_manager.start_transaction(2)
    transaction_manager.rollback_transaction(2)

    db_handler = DBHandler()
    db_handler.read_database()
    db_handler.buffer[0] = 1
    db_handler.write_database()