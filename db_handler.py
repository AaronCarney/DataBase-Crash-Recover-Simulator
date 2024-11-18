from logging_config import get_logger
import os


class DBHandler:
    def __init__(self, db_file="db"):
        """
        Initialize the DBHandler.
        - db_file: Name of the file to store the database.
        """
        self.db_file = db_file
        self.buffer = [0] * 32  # Simulated database (32 bits, all initialized to 0)
        self.write_count = 0  # Track number of writes since the last flush
        self.flush_threshold = 25  # Flush database to disk after this many writes
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"DBHandler initialized with database file {self.db_file}.")

    def read_database(self):
        """
        Read the database from the file into memory.
        If the file is missing or corrupted, initialize with default values.
        """
        try:
            if not os.path.exists(self.db_file):
                self.logger.warning("Database file not found. Initializing with default values.")
                self.buffer = [0] * 32
                return

            with open(self.db_file, "r") as f:
                line = f.readline().strip()
                if not line:  # Empty file
                    self.logger.warning("Database file is empty. Initializing with default values.")
                    self.buffer = [0] * 32
                else:
                    self.buffer = list(map(int, line.split(",")))
            self.logger.info("Database loaded from file.")
        except (FileNotFoundError, ValueError):
            self.logger.error("Invalid or missing database file. Initializing with default values.")
            self.buffer = [0] * 32

    def write_database(self):
        """
        Write the current database buffer to the file.
        This function is explicitly called after a recovery or periodic flush.
        """
        try:
            with open(self.db_file, "w", encoding="utf-8") as f:
                f.write(",".join(map(str, self.buffer)) + "\n")
            self.logger.info("Database written to file.")
            self.write_count = 0  # Reset write count after a flush
        except Exception as e:
            self.logger.error(f"Error writing to database file: {e}")

    def update_buffer(self, data_id, new_value):
        """
        Update a specific entry in the database buffer.
        Triggers a flush if the flush threshold is reached.
        - data_id: Index of the database entry to update.
        - new_value: The new value to assign.
        """
        if data_id < 0 or data_id >= len(self.buffer):
            self.logger.error(f"Invalid data_id {data_id}. No update performed.")
            return False

        old_value = self.buffer[data_id]
        self.buffer[data_id] = new_value
        self.logger.info(f"Database buffer updated at index {data_id}: {old_value} -> {new_value}.")
        self.write_count += 1

        # Flush to disk if threshold is reached
        if self.write_count >= self.flush_threshold:
            self.logger.info("Flush threshold reached. Writing database to disk.")
            self.write_database()
        return True