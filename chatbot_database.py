import sqlite3                  # SQL database
import json                     # To load lines from data dump
from datetime import datetime   # Optional for logging

timeframe = '2018-09'   # YYYY-MM of dataset to use

# Build up statements to execute in a single SQL transaction and commit
# in bulk groups to avoid millions of individual commits (costly)
sql_transaction = []

# Create the SQLite database if it doesn't exist
connection = sqlite3.connect('.db/{}.db'.format(timeframe))
c = connection.cursor()

# Create a table with the desired keys
def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

# Only execute the following code when running this file directly as the `main`
# program, not when importing this as a module into another file
if __name__ == '__main__':
    create_table()