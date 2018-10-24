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

# Normalize comments and convert newline character to a word for tokenizing
def format_data(data):
    data = data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return data

# Find a comment's parent so that you can pair them up
def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))   # Uncomment for debugging
        return False

# Only execute the following code when running this file directly as the `main`
# program, not when importing this as a module into another file
if __name__ == '__main__':
    create_table()
    row_counter = 0 # Will output at intervals to track progress through file
    paired_rows = 0 # Only interested in comments in a parent-reply pair

    # Files are too large to handle in memory, so need to use `buffering` param
    with open("./chatdata/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)   # Convert JSON to Python Object
            parent_id = row['parent_id'].split('_')[1]
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['id']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)