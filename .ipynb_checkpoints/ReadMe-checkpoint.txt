ADBSim: Simulation Setup and Usage Instructions

1. Overview
   The ADBSim project simulates a recovery manager and a lock manager using the concepts of strict two-phase locking and write-ahead logging (WAL). The database consists of 32 bits, initially set to 0. This simulation handles transactions and ensures consistency even in cases of crashes or deadlocks.

2. Setup
   - Ensure Python 3.13.0 is installed.
   - Place all project files in a directory named ADBSim.
   - There are no external libraries that require installation. 
        - The project exclusively uses standard Python libraries like:
            * argparse, 
            * unittest, 
            * logging, and 
            * built-in file handling

3. Run the Simulation
   - Use the following command:
     python main.py <cycles> <trans_size> <start_prob> <write_prob> <rollback_prob> <timeout>

4. Parameter Explanation
   - cycles: Total simulation cycles (integer, e.g., 100).
   - trans_size: Max operations per transaction (integer, e.g., 5).
   - start_prob: Probability of starting a transaction (float, 0–1).
   - write_prob: Probability of a write operation (float, 0–1).
   - rollback_prob: Probability of rolling back a transaction (float, 0–1).
   - timeout: Timeout for blocked transactions in cycles (integer, e.g., 10).

5. Example Command
   - Example: python main.py 50 3 0.7 0.5 0.2 5
     This runs 50 cycles with:
     - 3 operations per transaction,
     - 70% chance to start a transaction each cycle,
     - 50% chance to write,
     - 20% chance to roll back, and
     - a 5-cycle timeout.

6. Outputs
   - Final database state printed to the console.
   - Log (log) and database (db) files in the directory.

7. Crash and Recovery
   - The simulation stops at the defined maximum cycles, simulating a crash.
   - The recovery manager replays committed transactions during the next run to ensure consistency.
