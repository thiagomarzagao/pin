import os
import pyodbc
import numpy as np
import pandas as pd
from rpy2 import robjects
from random import randint
from datetime import datetime
from statsmodels.tsa.stattools import adfuller

# path to output data
path = '/vpins/'

# connect to database
cnxn = pyodbc.connect(
    driver = 'ODBC Driver 17 for SQL Server',
    server = '',
    database = '',
    uid = '',
    pwd = '',
    chartset = 'UTF-8'
)
cursor = cnxn.cursor()

# get all tickers
tickers = []
for row in cursor.tables():
    table_name = row.table_name
    if len(table_name) in (5, 6):
        if table_name[:4].isalpha():
            if table_name[:4].isupper():
                if table_name[4:].isnumeric():
                    tickers.append(table_name)
tickers = sorted(tickers)

# ignore already computed
done = []
for fname in os.listdir(path):
    ticker = fname.replace('.csv', '')
    done.append(ticker)

# get start and end of each quarter
quarters = [
    ('2019Q4', 61, datetime(2019, 10, 1), datetime(2019, 12, 31)),
    ('2020Q1', 62, datetime(2020, 1, 1), datetime(2020, 3, 31)),
    ('2020Q2', 61, datetime(2020, 4, 1), datetime(2020, 6, 30)),
    ('2020Q3', 65, datetime(2020, 7, 1), datetime(2020, 9, 30)),
    ('2020Q4', 61, datetime(2020, 10, 1), datetime(2020, 12, 31)),
    ('2021Q1', 60, datetime(2021, 1, 1), datetime(2021, 3, 31))
]

# check if buy or sell transaction
def buy_or_sell(flag):
    '''
    see https://www.mql5.com/en/forum/75268
    for explanation on MetaTrader flags
    '''
    if (flag & 32) and (flag & 64):
        return 'both'
    elif flag & 32:
        return 'buy'
    elif flag & 64:
        return 'sell'
    else:
        return 'neither' # shouldn't happen w/ trade ticks

# how many buckets to use in each update?
n = 250

# loop through every quarter and ticker
for i, ticker in enumerate(tickers):

    if ticker in done:
        continue

    # initialize counters
    total_volume = 0
    buy_volume = 0
    sell_volume = 0
    buckets = []
    output = []

    # get V
    query = '''
    SET DATEFORMAT ymd;

    SELECT 
        CAST(ticktime AS date) AS date, 
        SUM(volume) AS volume 
    FROM dbo.[{}] 
    GROUP BY CAST(ticktime AS date) 
    ORDER BY CAST(ticktime AS date)
    '''.format(ticker)
    df = pd.read_sql(query, cnxn)
    if df.shape[0]:
        avg_daily_vol = df['volume'].sum() / df.shape[0]
        V = int(avg_daily_vol / 50) # set V to 1/50th of avg daily volume
    else:
        continue

    # loop through quarters
    for tup in quarters:
        quarter = tup[0]
        trading_days = tup[1]
        date_start = tup[2].strftime('%Y-%m-%d')
        date_end = tup[3].strftime('%Y-%m-%d')
        print(' ')
        print(i, quarter, ticker)

        # load tick data
        query = '''
        SET DATEFORMAT ymd;  

        SELECT 
            [ticktime], 
            [flags],
            [volume]
        FROM [db_stonks].[dbo].[{}]
        WHERE [ticktime] >= CAST(N'{} 00:00:00' AS DateTime)
        AND [ticktime] <= CAST(N'{} 23:59:59' AS DateTime)
        '''.format(ticker, date_start, date_end)
        try:
            df = pd.read_sql(query, cnxn)
        except MemoryError:
            l = ','.join([
                ticker, 
                quarter, 
                ' ', 
                ' ',
                ' ' ,
                'MemoryError',
                '\n'
                ])
            with open('log.txt', mode = 'a') as f:
                f.write(l)
            print('MemoryError')
            continue

        # drop if zero data
        if df.shape[0] == 0:
            l = ','.join([
                ticker, 
                quarter, 
                ' ', 
                ' ',
                ' ',
                'nodata',
                '\n'
                ])
            with open('log.txt', mode = 'a') as f:
                f.write(l)
            print('nodata')
            continue

        # fix time column
        df['ticktime'] = pd.to_datetime(df['ticktime'])
        df = df.sort_values(by = 'ticktime')
        df.set_index('ticktime', inplace = True)

        # add column saying if buy or sell
        df['trade'] = df['flags'].apply(buy_or_sell)
        del df['flags']

        # drop simultaneous transactions (buy AND sell)
        df = df[df['trade'] != 'both']

        # drop non-transactions (shouldn't happen but who knows)
        df = df[df['trade'] != 'neither']

        # recode buy/sell flags
        df['B'] = df['trade'].map(lambda x: 1 if x == 'buy' else 0)
        df['S'] = df['trade'].map(lambda x: 1 if x == 'sell' else 0)
        del df['trade']

        # VPIN algorithm
        for row in df.iterrows():

            # calculate VPIN
            if len(buckets) == n:
                imbalance = 0
                for bucket in buckets:
                    diff = bucket[0] - bucket[1]
                    imbalance += abs(diff)
                vpin = imbalance / (n * V)

                # sanity check
                if (vpin < 0) or (vpin > 1):
                    print('ALL HELL BROKE LOOSE!')
                    quit()

                output.append((row[0], vpin))

                # discard first bucket
                buckets = buckets[1:]

            # excess volume from before triggers V?
            if total_volume >= V:

                # get excess volume
                excess = total_volume - V

                # store new bucket
                if buy:
                    buckets.append((0, V))
                else:
                    buckets.append((V, 0))

                # put excess volume into next bucket
                total_volume = excess
                if buy:
                    buy_volume = excess
                    sell_volume = 0
                else:
                    sell_volume = excess
                    buy_volume = 0
                continue

            # get tick's volume
            new_volume = row[1]['volume']

            # get direction
            if row[1]['B'] == 1:
                buy = True
            else:
                buy = False

            # triggers V?
            if total_volume + new_volume >= V:

                # get excess volume
                excess = (total_volume + new_volume) - V

                # store new bucket
                if buy:
                    buckets.append((sell_volume, V - sell_volume))
                else:
                    buckets.append((V - buy_volume, buy_volume))

                # put excess volume into next bucket
                total_volume = excess
                if buy:
                    buy_volume = excess
                    sell_volume = 0
                else:
                    sell_volume = excess
                    buy_volume = 0
                continue

            # add new volume to current bucket
            total_volume += new_volume
            if buy:
                buy_volume += new_volume
            else:
                sell_volume += new_volume


    if len(output) > 0:
        output = pd.DataFrame(output)
        output.columns = ['timestamp', 'vpin']
        output.set_index('timestamp', inplace = True)
        output.to_csv(path + ticker + '.csv')