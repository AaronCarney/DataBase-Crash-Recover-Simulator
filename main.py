from db_handler import DBHandler
from lock_manager import LockManager
from logging_config import setup_logging, get_logger
from recovery_manager import RecoveryManager
from transaction_manager import TransactionManager
import argparse
import random
from time import sleep
from typing import Tuple


def initialize_modules() -> Tuple[DBHandler, RecoveryManager, LockManager, TransactionManager]:
    """
    Initialize the database, logs, and all necessary modules for the simulation.
    Returns:
        Tuple of initialized modules: (database_handler, recovery_mgr, lock_mgr, transaction_mgr)
    """
    logger = get_logger("Initializer")
    logger.info("Starting module initialization...")

    # Initialize logging
    setup_logging()
    logger.info("Logging system initialized.")

    # Initialize the database handler
    database_handler = DBHandler()
    database_handler.read_database()  # Load database from file or initialize to defaults
    logger.info("Database handler initialized and database state loaded.")

    # Initialize the recovery manager and apply logs
    recovery_mgr = RecoveryManager()
    recovery_mgr.apply_logs(database_handler)  # Ensure database state is consistent
    logger.info("Recovery manager initialized and logs applied.")

    # Initialize the lock manager
    lock_mgr = LockManager()
    logger.info("Lock manager initialized.")

    # Initialize the transaction manager
    transaction_mgr = TransactionManager(lock_mgr, recovery_mgr, database_handler)
    logger.info("Transaction manager initialized.")

    logger.info("Module initialization complete.")
    return database_handler, recovery_mgr, lock_mgr, transaction_mgr


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


def simulation_loop(
        db_handler, recovery_manager, lock_manager, transaction_manager,
        max_cycles, max_transaction_size, prob_start_transaction, prob_write,
        prob_rollback, lock_timeout
):
    """
    Run the simulation loop for managing transactions, locks, and recovery.

    Args:
        db_handler: Instance of the DBHandler class.
        recovery_manager: Instance of the RecoveryManager class.
        lock_manager: Instance of the LockManager class.
        transaction_manager: Instance of the TransactionManager class.
        max_cycles: Total number of cycles for the simulation.
        max_transaction_size: Maximum operations per transaction.
        prob_start_transaction: Probability of starting a new transaction.
        prob_write: Probability that an operation is a write.
        prob_rollback: Probability that an operation is a rollback.
        lock_timeout: Maximum wait time for transactions before timeout.
    """
    logger = get_logger("SimulationLoop")
    logger.info("Starting simulation loop...")

    active_transactions = {}
    current_cycle = 0

    while current_cycle < max_cycles:
        logger.info(f"Cycle {current_cycle + 1} begins.")

        # Start a new transaction based on prob_start_transaction
        if random.random() <= prob_start_transaction:
            transaction_id = len(active_transactions) + 1
            transaction_manager.start_transaction(transaction_id)
            active_transactions[transaction_id] = {
                "operations_count": 0,
                "is_blocked": False,
                "start_time": current_cycle
            }
            logger.info(f"Started transaction {transaction_id}.")

        # Process active transactions
        for transaction_id, transaction_data in list(active_transactions.items()):
            if transaction_data["is_blocked"]:
                logger.debug(f"Transaction {transaction_id} is blocked, skipping.")
                continue

            if transaction_data["operations_count"] >= max_transaction_size:
                transaction_manager.commit_transaction(transaction_id)
                logger.info(f"Committed transaction {transaction_id}.")
                del active_transactions[transaction_id]
                continue

            operation_type = (
                "rollback" if random.random() <= prob_rollback
                else "write" if random.random() <= prob_write
                else "noop"
            )

            if operation_type == "rollback":
                transaction_manager.rollback_transaction(transaction_id)
                logger.info(f"Transaction {transaction_id} rolled back.")
                del active_transactions[transaction_id]
            elif operation_type == "write":
                data_id = random.randint(0, 31)
                old_value = db_handler.buffer[data_id]
                new_value = 1 if old_value == 0 else 0
                success = transaction_manager.submit_operation(transaction_id, data_id, "F", old_value, new_value)
                if success:
                    transaction_data["operations_count"] += 1
                    logger.info(f"Transaction {transaction_id} wrote to data {data_id}.")
                else:
                    transaction_data["is_blocked"] = True

        # Resolve deadlocks with lock_timeout
        lock_manager.check_deadlocks(lock_timeout)

        # Flush logs and database after every 25 writes
        if recovery_manager.write_count >= 25:
            recovery_manager.flush_logs()
            db_handler.write_database()

        # Increment cycle count
        current_cycle += 1
        sleep(0.1)  # Simulate delay

    logger.info("Simulation loop complete. Final database state:")
    print(db_handler.buffer)


if __name__ == "__main__":
    # Set up logging
    setup_logging()
    main_logger = get_logger("main")
    main_logger.info("Logging is set up and working correctly.")

    # Parse command-line arguments
    simulation_args = parse_arguments()

    # Initialize modules
    db_handler_instance, recovery_manager_instance, lock_manager_instance, transaction_manager_instance = \
        initialize_modules()

    # Simulation parameters from parsed arguments
    total_cycles = simulation_args.cycles
    transaction_size = simulation_args.trans_size
    start_probability = simulation_args.start_prob
    write_probability = simulation_args.write_prob
    rollback_probability = simulation_args.rollback_prob
    timeout_duration = simulation_args.timeout

    # Start simulation loop with updated argument names
    simulation_loop(
        db_handler_instance, recovery_manager_instance, lock_manager_instance, transaction_manager_instance,
        total_cycles, transaction_size, start_probability, write_probability,
        rollback_probability, timeout_duration
    )

    main_logger.info("Simulation successfully completed.")
    print("Modules successfully initialized and simulation completed.")
