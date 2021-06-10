import re
import math
import pandas as pd
from scipy.stats import pearsonr
from matplotlib import pyplot as plt

# load estimates
path = '/Users/thiagomarzagao/Dropbox/dataScience/insider/code/'
df_lk = pd.read_csv(path + 'pin_gan_lk_estimates.csv')
df_eho = pd.read_csv(path + 'pin_gan_eho_estimates.csv')
df_ea = pd.read_csv(path + 'pin_ea_lk_estimates.csv')

# compare GAN-LK vs GAN-EHO estimates
df_lkeho = pd.merge(
    df_lk, 
    df_eho, 
    how = 'inner', 
    on = ['ticker', 'quarter'],
    suffixes = ['_lk', '_eho']
    )
df_lkeho['PIN_lk'] = df_lkeho['PIN_lk'].map(lambda x: math.log(x+0.01))
df_lkeho['PIN_eho'] = df_lkeho['PIN_eho'].map(lambda x: math.log(x+0.01))
df_lkeho['diff'] = df_lkeho['PIN_lk'] - df_lkeho['PIN_eho']
df_lkeho['ticks'] = df_lkeho['B_sum_lk'] + df_lkeho['S_sum_lk']
df_lkeho = df_lkeho[['diff', 'ticks']]
print(pearsonr(df_lkeho['diff'], df_lkeho['ticks']))
df_lkeho.plot.scatter('ticks', 'diff', c = 'black')
plt.xlabel('number of trades')
plt.ylabel('ln(LK PIN) - ln(EHO PIN)')
plt.show()

# compare GAN-LK vs EA-LK estimates
df_ganea = pd.merge(
    df_lk, 
    df_ea,
    how = 'inner',
    on = ['ticker', 'quarter'],
    suffixes = ['_gan', '_ea']
    )
df_ganea['PIN_gan'] = df_ganea['PIN_gan'].map(lambda x: math.log(x+0.01))
df_ganea['PIN_ea'] = df_ganea['PIN_ea'].map(lambda x: math.log(x+0.01))
df_ganea['diff'] = df_ganea['PIN_gan'] - df_ganea['PIN_ea']
df_ganea['ticks'] = df_ganea['B_sum_gan'] + df_ganea['S_sum_gan']
df_ganea = df_ganea[['diff', 'ticks']]
print(pearsonr(df_ganea['diff'], df_ganea['ticks']))
df_ganea.plot.scatter('ticks', 'diff', c = 'black')
plt.xlabel('number of trades')
plt.ylabel('ln(GAN PIN) - ln(EA PIN)')
plt.show()

# check PIN summary stats
print(df_lk['PIN'].describe())

# plot PIN distribution
ax = df_lk['PIN'].plot.hist(bins = 100, color = 'black')
ax.set_ylabel('frequency')
ax.set_xlabel('PIN')
plt.show()

# check PIN vs volume
df_lk['ticks'] = df_lk['B_sum'] + df_lk['S_sum']
print(pearsonr(df_lk['ticks'], df_lk['PIN']))

def fix_quarter(s):
    '''
    _2019_4Q -> 2019Q4
    '''
    year, quarter = s[1:5], s[6:][::-1]
    return year + quarter

# load governance levels
gov = pd.read_csv(path + 'governance.csv')
gov['quarter'] = gov['quarter'].apply(fix_quarter)
df_lk['subticker'] = df_lk['ticker'].map(lambda x: x[:4])
df_lk = pd.merge(df_lk, gov, how = 'left', on = ['subticker', 'quarter'])
df_lk['governance'] = df_lk['governance'].fillna('Básico')

# check PIN vs governance levels
PIN_NM = df_lk[df_lk['governance'] == 'NM']['PIN'].mean()
PIN_nonNM = df_lk[df_lk['governance'] != 'NM']['PIN'].mean()
print(PIN_NM, PIN_nonNM)

# check all that again, but for each quarter separately
for quarter, dt in df_lk.groupby(['quarter']):
    print(' ')
    print(quarter)
    print(dt['PIN'].describe())
    print(pearsonr(dt['ticks'], dt['PIN']))
    PIN_NM = dt[dt['governance'] == 'NM']['PIN'].mean()
    PIN_nonNM = dt[dt['governance'] != 'NM']['PIN'].mean()
    print(PIN_NM, PIN_nonNM)

# load B3code<->CNPJ correspondence table
cnpjs = pd.read_csv('cnpj_to_b3.csv', usecols = ['cnpj', 'codigo_B3'])
cnpjs['cnpj'] = cnpjs['cnpj'].map(lambda x: re.sub('[^0-9]', '', str(x).zfill(14)))
cnpjs['subticker'] = cnpjs['codigo_B3'].map(lambda x: x[:4])
del cnpjs['codigo_B3']
cnpjs = cnpjs.drop_duplicates(subset = ['subticker'], keep = 'first')

# get each ticker's CNPJ
df_lk = pd.merge(df_lk, cnpjs, on = 'subticker', how = 'left')

# load CNPJ<->CNAE correspondence table
cnae = pd.read_csv('cnpj_to_cnae.csv', dtype = {'cnpj': str, 'natjur': str, 'cnae': str})
cnae['cnae'] = cnae['cnae'].map(lambda x: x[:5])

# get each ticker's CNAE
df_lk = pd.merge(df_lk, cnae, on = 'cnpj', how = 'left')
df_lk['cnae'] = df_lk['cnae'].map(lambda x: str(x)[:3])

# get average PIN for each CNAE
cnaes = []
for cnae in set(df_lk['cnae']):
    subdf = df_lk[df_lk['cnae'] == cnae]
    avgpin = subdf['PIN'].mean()
    cnaes.append((cnae, avgpin, subdf.shape[0]))
cnaes = pd.DataFrame(cnaes)
cnaes.columns = ['cnae', 'PIN', 'count']
cnaes = cnaes.sort_values(by = 'PIN')
print(cnaes)
