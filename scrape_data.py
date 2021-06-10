import os
import time
import random
import calendar
import pandas as pd
import MetaTrader5 as mt5
from sqlalchemy import create_engine, inspect
from datetime import datetime, timedelta

# connect to SQL Server
engine = create_engine('mssql+pyodbc://SqlServerName/DbName?driver=SQL+Server')
cnxn = engine.raw_connection()
cursor = cnxn.cursor()

# connect to MetaTrader 5
if not mt5.initialize():
    print('initialize() failed')
    mt5.shutdown()
 
# get connection status and parameters
print(mt5.terminal_info())

# get MetaTrader 5 version
print(mt5.version())

# path to temp data (so we can delete it)
path_to_tmp = 'C:/path/to/temp/data/'

# get all B3 symbols
symbols = mt5.symbols_get()

# get all stocks;
# this discards derivatives: forward market, 
# futures, options, swaps, OEs;
# this also discards units and BDRs
tickers = []
group1 = [str(e) for e in range(3, 9)]
group2 = [str(e) for e in range(9, 11)]
for symbol in symbols:
    ticker = symbol.name
    if not ticker[:4].isalpha():
        continue
    if (len(ticker) == 5) and (ticker[-1] in group1):
        tickers.append(ticker)
    elif (len(ticker) == 6) and (ticker[-2:] in group2):
        tickers.append(ticker)

# check which tables are already there
inspector = inspect(engine)
tables = []
for table_name in inspector.get_table_names():
    tables.append(table_name)

# month-years to scrape
months = {
    2019: (10, 11, 12),
    2020: range(1, 13),
    2021: (1, 2, 3)
}

# loop through tickers
start = time.time()
for i, ticker in enumerate(tickers):

    # drop empty tables
    if ticker in tables:
        query = '''
        SELECT COUNT(*) FROM {}
        '''.format(ticker)
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print('count:', ticker, count)
        if count == 0:
            query = '''
            DROP TABLE {}
            '''.format(ticker)
            cursor.execute(query)
            cnxn.commit()
        else:
            continue

    # create table for ticker
    query = '''
    CREATE TABLE {} (
        ticktime datetime,
        bid smallmoney,
        ask smallmoney,
        last smallmoney,
        volume int,
        time_msc bigint,
        flags smallint,
        volume_real int
    );
    '''.format(ticker)
    cursor.execute(query)
    cnxn.commit()

    # loop through month-years
    for year in months.keys():
        for month in months[year]:

            # set date range
            last_day = calendar.monthrange(year, month)[1]

            # loop through days
            for day in range(1, last_day + 1):
                print(' ')
                print(i, 'of', len(tickers), ticker, year, month, day)
                t0 = datetime(year, month, day, 0, 0, 0)
                t1 = datetime(year, month, day, 23, 59, 0)

                # request tick data
                ticks = mt5.copy_ticks_range(
                    ticker, 
                    t0, 
                    t1, 
                    mt5.COPY_TICKS_TRADE
                    )
                ticks = pd.DataFrame(ticks)

                # log if results are empty
                if ticks.shape[0] == 0:
                    with open('log.txt', mode = 'a') as f:
                        l = ticker + ',' + str(year) + ',' + str(month) + ',' + str(day) + '\n'
                        f.write(l)
                        print('empty DataFrame:', l)
                        continue            # persist

                print(ticks.shape[0])
                ticks['time'] = pd.to_datetime(ticks['time'], unit = 's')
                ticks.columns = [
                    'ticktime',
                    'bid',
                    'ask',
                    'last',
                    'volume',
                    'time_msc',
                    'flags',
                    'volume_real'
                    ]
                ticks.to_sql(
                    ticker,
                    con = engine,
                    if_exists = 'append',
                    index = False
                    )

                # commit changes
                cnxn.commit()

                # don't over-request
                time.sleep(2.5 + random.random())

    # delete tmp files
    '''
    for fname in os.listdir(path_to_tmp + ticker + '/'):
        try:
            os.remove(path_to_tmp + ticker + '/' + fname)
        except:
            pass
    '''

    # how long did it take?
    elapsed = time.time() - start
    print('it took', round(elapsed / 60), 'minutes')

# shut down connection to MetaTrader 5
mt5.shutdown()

# shut down connection to SQL Server
cnxn.close()
