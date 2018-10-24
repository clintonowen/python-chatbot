import sqlite3
# Don't _need_ pandas for this, but it may come
# in handy if you want to add more logic later on
import pandas as pd

# List will give you the ability to add more later
timeframes = ['2018-09']

def writeTxt(filename, key):
    with open(filename, 'a', encoding='utf8') as f:
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
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        if not test_done:
            writeTxt('test.from', 'parent')
            writeTxt('test.to', 'comment')
            test_done = True
        else:
            writeTxt('train.from', 'parent')
            writeTxt('train.to', 'comment')
        counter += 1
        if counter % 20 == 0:
            print(counter*limit,'rows completed so far')