timeframe = '2018-09'

with open("../chatdata/{}/RC_{}".format(timeframe.split('-')[0], timeframe), 'r') as f2:
    row_counter = 0
    for row in f2:
        row_counter += 1
    print(row_counter)