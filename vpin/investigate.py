import ssl
import pandas as pd

# load data
ssl._create_default_https_context = ssl._create_unverified_context
url = 'https://raw.githubusercontent.com/thiagomarzagao/pin/main/pin_gan_lk_estimates.csv'
df = pd.read_csv(url)

# select stock-quarters w/ highest PIN values
df = df.sort_values(by = ['PIN'], ascending = False)[:15]

# for each selected stock-quarter, get highest VPIN values
data = []
for i, row in df.iterrows():
    ticker = row['ticker']
    pin = row['PIN']
    url = 'https://raw.githubusercontent.com/thiagomarzagao/pin/main/vpins/{}.csv'.format(ticker)
    try:
        df_ticker = pd.read_csv(url)
    except:
        continue
    df_ticker = df_ticker.sort_values(by = ['vpin'])[:15]
    for j, nrow in df_ticker.iterrows():
        tup = (ticker, pin, nrow['timestamp'], nrow['vpin'])
        data.append(tup)

data = pd.DataFrame(data)
data.columns = ['ticker', 'PIN', 'timestamp', 'VPIN']
data.to_csv('to_investigate.csv', index = False)
print(data)