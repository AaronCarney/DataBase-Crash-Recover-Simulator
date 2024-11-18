import unittest
import os
from db_handler import DBHandler


class TestDBHandler(unittest.TestCase):
    def setUp(self):
        self.db_file = "test_db"
        self.db_handler = DBHandler(db_file=self.db_file)
        # Ensure a clean slate for the database file
        if os.path.exists(self.db_handler.db_file):
            os.remove(self.db_handler.db_file)

    def test_read_database(self):
        self.db_handler.read_database()
        self.assertEqual(self.db_handler.buffer, [0] * 32)

    def test_write_database(self):
        self.db_handler.buffer[0] = 1
        self.db_handler.write_database()
        with open(self.db_handler.db_file, "r") as f:
            content = f.readline().strip()
        self.assertEqual(content, "1," + ",".join(["0"] * 31))

    def test_empty_database_file(self):
        """Test handling an empty database file."""
        with open(self.db_handler.db_file, "w") as f:
            f.write("")  # Create an empty database file
        self.db_handler.read_database()
        self.assertEqual(self.db_handler.buffer, [0] * 32)

    def test_corrupted_database_file(self):
        """Test handling a corrupted database file."""
        with open(self.db_handler.db_file, "w") as f:
            f.write("invalid,data,content")
        self.db_handler.read_database()
        self.assertEqual(self.db_handler.buffer, [0] * 32)  # Should default to zeros

    def test_buffer_update_and_flush(self):
        """Test that updates to the buffer are correctly written to the file."""
        self.db_handler.buffer[0] = 1
        self.db_handler.write_database()
        with open(self.db_handler.db_file, "r") as f:
            content = f.readline().strip()
        self.assertEqual(content, "1," + ",".join(["0"] * 31))


if __name__ == "__main__":
    unittest.main()