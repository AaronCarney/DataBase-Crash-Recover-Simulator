from db_handler import DBHandler
from lock_manager import LockManager
from logging_config import setup_logging, get_logger
from recovery_manager import RecoveryManager
from transaction_manager import TransactionManager
import argparse


def parse_arguments():
    """
    Parse command-line arguments for the simulation.
    Returns:
        Namespace containing all validated parameters.
    """
    parser_logger = get_logger("ArgumentParser")  # Use a distinct name for the logger

    parser = argparse.ArgumentParser(
        description="Simulate recovery and locking with strict 2PL and WAL."
    )

    # Define arguments
    parser.add_argument(
        "cycles", type=int,
        help="Maximum number of cycles for the simulation (integer > 0)."
    )
    parser.add_argument(
        "trans_size", type=int,
        help="Number of operations per transaction (integer > 0)."
    )
    parser.add_argument(
        "start_prob", type=float,
        help="Probability of starting a new transaction per cycle (0 <= value <= 1)."
    )
    parser.add_argument(
        "write_prob", type=float,
        help="Probability of a write operation per transaction (0 <= value <= 1)."
    )
    parser.add_argument(
        "rollback_prob", type=float,
        help="Probability of a rollback operation per transaction (0 <= value <= 1)."
    )
    parser.add_argument(
        "timeout", type=int,
        help="Timeout in cycles for transactions waiting for resources (integer >= 0)."
    )

    # Parse the arguments
    parsed_args = parser.parse_args()  # Use a distinct name for the parsed arguments

    # Validate probabilities
    if not (0 <= parsed_args.start_prob <= 1):
        parser.error("start_prob must be between 0 and 1.")
    if not (0 <= parsed_args.write_prob <= 1):
        parser.error("write_prob must be between 0 and 1.")
    if not (0 <= parsed_args.rollback_prob <= 1):
        parser.error("rollback_prob must be between 0 and 1.")
    if parsed_args.write_prob + parsed_args.rollback_prob > 1:
        parser.error("write_prob + rollback_prob must not exceed 1.")

    # Log the parsed arguments
    parser_logger.info(f"Parsed arguments: {vars(parsed_args)}")

    return parsed_args


if __name__ == "__main__":
    setup_logging()
    main_logger = get_logger("main")  # Use a distinct name for the main logger
    main_logger.info("Logging is set up and working correctly.")

    # Parse command-line arguments
    simulation_args = parse_arguments()  # Use a distinct name for the arguments

    # Initialize components
    db_handler = DBHandler()
    db_handler.read_database()
    db_handler.buffer[0] = 1
    db_handler.write_database()

    lock_manager = LockManager()
    lock_manager.acquire_lock(1, "data1", "shared")
    lock_manager.acquire_lock(2, "data1", "exclusive")
    lock_manager.release_locks(1)
    lock_manager.check_deadlocks()

    recovery_manager = RecoveryManager()
    recovery_manager.write_log(1, "S")
    recovery_manager.write_log(1, "F", data_id=0, old_value=0, new_value=1)
    recovery_manager.apply_logs(db_handler)

    transaction_manager = TransactionManager(lock_manager, recovery_manager, db_handler)
    transaction_manager.start_transaction(1)
    transaction_manager.submit_operation(1, "data1", "F", old_value=0, new_value=1)
    transaction_manager.commit_transaction(1)
    transaction_manager.start_transaction(2)
    transaction_manager.rollback_transaction(2)

    main_logger.info("Simulation successfully initialized.")
