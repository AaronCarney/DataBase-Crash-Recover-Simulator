from logging_config import get_logger

class DBHandler:
    def __init__(self, db_file="db.txt"):
        """Initialize the DBHandler."""
        self.db_file = db_file
        self.buffer = [0] * 32  # Simulated database (32 bits, all initialized to 0)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"DBHandler initialized with database file {self.db_file}.")

    def read_database(self):
        """Read the database from the file into memory."""
        try:
            with open(self.db_file, "r") as f:
                line = f.readline().strip()
                if not line:  # File is empty
                    self.logger.warning("Database file is empty. Initializing with default values.")
                    self.buffer = [0] * 32
                else:
                    self.buffer = list(map(int, line.split(",")))
            self.logger.info("Database loaded from file.")
        except FileNotFoundError:
            self.logger.warning("Database file not found. Initializing with default values.")
            self.buffer = [0] * 32
        except ValueError as e:
            self.logger.error(f"Error reading database file: {e}. Initializing with default values.")
            self.buffer = [0] * 32

    def write_database(self):
        """Write the current database buffer to the file."""
        with open(self.db_file, "w") as f:
            f.write(",".join(map(str, self.buffer)) + "\n")
        self.logger.info("Database written to file.")

