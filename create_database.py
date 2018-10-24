import sqlite3                  # SQL database
import json                     # To load lines from data dump
from datetime import datetime   # Optional for logging

timeframe = '2018-09'   # YYYY-MM of dataset to use
# timeframe = ['2018-09'] # future iterable list of datasets to use

# Build up statements to execute in a single SQL transaction and commit
# in bulk groups to avoid millions of individual commits (costly)
sql_transaction = []

# If you need to stop and restart database inserting,
# set the `start_row` to continue where you left off
start_row = 0

# Remove any comments without parents at every 1M rows to
# remove bloat and keep insertion speeds high.
# This will result in an acceptable loss of ~2K pairs per
# every ~100K pairs found per every ~1M rows processed.
cleanup = 1000000 

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

# Only allow comments of acceptable length which have not been deleted/removed
def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True

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

# Check if a parent has an existing comment and return its score
def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))   # Uncomment for debugging
        return False

# Commit statements in bulk groups of 1000 to avoid millions of costly commits
def transaction_bldr(sql):
    global sql_transaction      # Make the variable available globally
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []    # Clear the list once a group has been committed

# Send an SQL query to update/replace an existing comment
def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = "UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id = ?;".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s-UPDATE insertion',str(e))

# Send an SQL query to add/insert a new comment/row _with_ parent data
def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion',str(e))

# Send an SQL query to add/insert a new comment/row with _no_ parent data
def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT insertion',str(e))

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

            if row_counter > start_row:
                try:
                    row = json.loads(row)   # Convert JSON to Python Object
                    parent_id = row['parent_id'].split('_')[1]
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    score = row['score']
                    comment_id = row['id']
                    subreddit = row['subreddit']
                    parent_data = find_parent(parent_id)

                    # If parent has existing comment with a lower score,
                    # replace it with the new comment
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            # Filter out comments of unacceptable length
                            if acceptable(body):
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        # Filter out comments of unacceptable length
                        if acceptable(body):
                            # If there is a parent but no existing comment
                            if parent_data:
                                # Only use comments with a score greater than 2
                                # (somebody thought it was good enough to upvote)
                                if score >= 2:
                                    sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                    paired_rows += 1
                            else:
                                # It might still be a parent to another comment
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                except Exception as e:
                    print(str(e))

            # Log the current status at every 100K rows
            if row_counter % 100000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))

            # Delete comments without parents at every `cleanup` # of rows
            if row_counter > start_row:
                if row_counter % cleanup == 0:
                    print("Cleaning up")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
    
    # Rebuild the database file, shrinking it to minimal amount of disk space
    c.execute("VACUUM")
    connection.commit()