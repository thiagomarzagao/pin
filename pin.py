import sys
import pyodbc
import numpy as np
import pandas as pd
from rpy2 import robjects
from random import randint
from datetime import datetime

# ID of process
pid = str(randint(1, 100000))

# path to output data
path = '/path/to/output/'

# connect to database
cnxn = pyodbc.connect(
    driver = 'ODBC Driver 17 for SQL Server',
    server = 'SqlServerName',
    database = 'DbName',
    uid = 'uid',
    pwd = 'pwd',
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

# get batch of tickers to process
batch = int(sys.argv[1])
tickers = np.array_split(tickers, 6)[batch].tolist()

# get start and end of each quarter
quarters = [
    ('2019Q4', 61, datetime(2019, 10, 1), datetime(2019, 12, 31)),
    ('2020Q1', 62, datetime(2020, 1, 1), datetime(2020, 3, 31)),
    ('2020Q2', 61, datetime(2020, 4, 1), datetime(2020, 6, 30)),
    ('2020Q3', 65, datetime(2020, 7, 1), datetime(2020, 9, 30)),
    ('2020Q4', 61, datetime(2020, 10, 1), datetime(2020, 12, 31)),
    ('2021Q1', 60, datetime(2021, 1, 1), datetime(2021, 3, 31))
]

# list out holidays
holidays = (
    '2019-10-12',
    '2019-11-02',
    '2019-11-15',
    '2019-12-24',
    '2019-12-25',
    '2019-12-31',
    '2019-11-20',
    '2020-01-01',
    '2020-02-24',
    '2020-02-25',
    '2020-04-10',
    '2020-04-21',
    '2020-05-01',
    '2020-06-11',
    '2020-09-07',
    '2020-10-12',
    '2020-11-02',
    '2020-12-24',
    '2020-12-25',
    '2020-12-31',
    '2021-01-01',
    '2021-01-25',
    '2021-02-16'
)

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

# loop through every quarter and ticker
all_estimates = []
for i, ticker in enumerate(tickers):
    for tup in quarters[::-1]: # get more recent quarters first
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
            [flags]
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
            with open('log{}.txt'.format(pid), mode = 'a') as f:
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
            with open('log{}.txt'.format(pid), mode = 'a') as f:
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

        # get sums
        B_sum = df['B'].sum()
        S_sum = df['S'].sum()

        # aggregate: ticks -> 1-day bars
        df = df.resample('1d').sum()

        # drop holidays
        for date in holidays:
            lb = date + ' 00:00:00'
            ub = date + ' 23:59:59'
            df = df[(df.index < lb) | (df.index > ub)]

        # drop weekends
        df = df[df.index.dayofweek < 5]

        # drop days with zero trades
        df = df[(df['B'] > 0) | (df['S'] > 0)]

        # get how many days stock was traded
        days_traded = df.shape[0]

        # stringify buy bars
        B = [str(e) for e in df['B'].values]
        B = ', '.join(B)
        B = 'c({})'.format(B)

        # stringify sell bars
        S = [str(e) for e in df['S'].values]
        S = ', '.join(S)
        S = 'c({})'.format(S)

        # estimate model parameters!
        rcode = '''
        library("InfoTrad")
        buy <- {}
        sell <- {}
        data <- as.data.frame(cbind(buy, sell))
        GAN(data, likelihood="LK")
        '''.format(B, S)
        try:
            estimates = robjects.r(rcode)
        except Exception as e:
            l = ','.join([
                ticker, 
                quarter, 
                str(B_sum),
                str(S_sum),
                'R_error',
                '\n'
                ])
            with open('log{}.txt'.format(pid), mode = 'a') as f:
                f.write(l)
            unitroot = True
            print(e)
            continue

        # append new row
        row = [ticker, quarter, B_sum, S_sum, days_traded]
        row += [e[0] for e in estimates]
        all_estimates.append(row)

df = pd.DataFrame(all_estimates)
df.columns = [
    'ticker',
    'quarter',
    'B_sum',
    'S_sum',
    'days_traded',
    'alpha',
    'delta',
    'mu',
    'epsilon_b',
    'epsilon_s',
    'likelihood',
    'PIN',
    ]
fname = 'pin_gan_lk_estimates_pid{}.csv'.format(pid)
df.to_csv(path + fname, index = False)
