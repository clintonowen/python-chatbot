import sqlite3
# Don't _need_ pandas for this, but it may come
# in handy if you want to add more logic later on
import pandas as pd

# List will give you the ability to add more later
timeframes = ['2018-09']

def writeTxt(filename, key):
    with open('./nmt-chatbot/new_data/{}'.format(filename), 'a', encoding='utf8') as f:
        for content in df[key].values:
            f.write(content+'\n')

for timeframe in timeframes:
    connection = sqlite3.connect('./db/{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000        # How much to pull at a time into pandas dataframe
    last_unix = 0       # Used for buffering when making database queries
    cur_length = limit  # Tells us when we're done
    counter = 0         # For logging our progress
    test_done = False   # For when we're done building testing data

    while cur_length == limit:
        # Get the first/next 5000 valid entries
        df = pd.read_sql('SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}'.format(last_unix, limit), connection)
        # Set "starting point" for next chunk as end of current one
        last_unix = df.tail(1)['unix'].values[0]
        # Update the cur_length to break the while loop when reaching the end
        cur_length = len(df)
        # Create test files using only the first chunk of data
        if not test_done:
            writeTxt('test.from', 'parent')
            writeTxt('test.to', 'comment')
            test_done = True
        # Write to training files for everything else
        else:
            writeTxt('train.from', 'parent')
            writeTxt('train.to', 'comment')
        counter += 1
        if counter % 10 == 0:
            print(counter*limit,'rows completed so far')