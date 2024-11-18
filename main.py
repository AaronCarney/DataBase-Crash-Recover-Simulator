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
    recovery_mgr = RecoveryManager(database_handler)
    recovery_mgr.apply_logs()
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
    """
    parser = argparse.ArgumentParser(
        description="Simulate recovery and locking with strict 2PL and WAL."
    )
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
    parser.add_argument(
        "prob_crash", type=float,
        help="Probability of a random crash per cycle (0 <= value <= 1)."
    )

    parsed_args = parser.parse_args()

    # Validate probabilities
    if not (0 <= parsed_args.start_prob <= 1):
        parser.error("start_prob must be between 0 and 1.")
    if not (0 <= parsed_args.write_prob <= 1):
        parser.error("write_prob must be between 0 and 1.")
    if not (0 <= parsed_args.rollback_prob <= 1):
        parser.error("rollback_prob must be between 0 and 1.")
    if not (0 <= parsed_args.prob_crash <= 1):
        parser.error("prob_crash must be between 0 and 1.")
    if parsed_args.write_prob + parsed_args.rollback_prob > 1:
        parser.error("write_prob + rollback_prob must not exceed 1.")

    return parsed_args


def simulation_loop(
        db_handler, recovery_manager, lock_manager, transaction_manager,
        max_cycles, max_transaction_size, prob_start_transaction, prob_write,
        prob_rollback, prob_crash
):
    """
    Run the simulation loop for managing transactions, locks, and recovery.
    """
    logger = get_logger("SimulationLoop")
    logger.info("Starting simulation loop...")

    active_transactions = {}
    current_cycle = 0

    while current_cycle < max_cycles:
        logger.info(f"Cycle {current_cycle + 1} begins.")

        # Random crash simulation
        if random.random() <= prob_crash:
            logger.error(f"Simulated crash occurred at cycle {current_cycle + 1}.")
            print(f"Simulated crash at cycle {current_cycle + 1}. Final database state: ")
            # print(db_handler.buffer)

            # Flush logs and database before crash
            recovery_manager.flush_logs()
            db_handler.write_database()

            # Terminate simulation
            break

        # Start a new transaction based on prob_start_transaction
        if random.random() <= prob_start_transaction:
            transaction_id = len(transaction_manager.transactions) + 1
            transaction_manager.start_transaction(transaction_id)
            active_transactions[transaction_id] = {
                "operations_count": 0,
                "is_blocked": False,
                "start_time": current_cycle
            }
            logger.info(f"Started transaction {transaction_id}.")

        # Process active transactions
        for transaction_id in list(active_transactions.keys()):
            transaction_data = active_transactions[transaction_id]
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
        lock_manager.check_deadlocks()

        # Flush logs and database after every 25 writes
        if recovery_manager.write_count >= 25:
            recovery_manager.flush_logs()
            db_handler.write_database()

        # Increment cycle count
        current_cycle += 1
        sleep(0.1)  # Simulate delay

    logger.info("Simulation loop complete. Final database state:")
    # Comment out the print statement to avoid output during tests
    # print(db_handler.buffer)

    # Rollback only active and uncommitted transactions
    for transaction_id in list(active_transactions.keys()):
        transaction_manager.rollback_transaction(transaction_id)
        logger.info(f"Transaction {transaction_id} rolled back due to simulation end.")
        del active_transactions[transaction_id]

    # Ensure all locks are released
    lock_manager.locks.clear()
    lock_manager.lock_queue.clear()
    lock_manager.locked_data_by_transaction.clear()
    lock_manager.transaction_timestamps.clear()

    # Flush any remaining logs and write database to disk
    recovery_manager.flush_logs()
    db_handler.write_database()


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
    crash_probability = simulation_args.prob_crash

    # Start simulation loop with updated argument names
    # Start simulation loop with updated argument names
    simulation_loop(
        db_handler_instance, recovery_manager_instance, lock_manager_instance, transaction_manager_instance,
        total_cycles, transaction_size, start_probability, write_probability,
        rollback_probability, crash_probability
    )

    main_logger.info("Simulation successfully completed.")
    print("Modules successfully initialized and simulation completed.")
