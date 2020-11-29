import bs4 as bs
import requests
import yfinance as yf
import datetime
import pandas as pd
import numpy as np

'''
Scraping the S&P 500 data from Wikipedia
'''
resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
soup = bs.BeautifulSoup(resp.text, 'lxml')
table = soup.find('table', {'class': 'wikitable sortable'})
tickers = []
names = []
sectors = []
sub_industries = []
hq_locations = []
dates_joined = []
ci_keys = []
dates_founded = []

for row in table.findAll('tr')[1:]:
    ticker = row.findAll('td')[0].text
    name = row.findAll('td')[1].text
    sector = row.findAll('td')[3].text
    sub_industry = row.findAll('td')[4].text
    hq_location = row.findAll('td')[5].text
    date_joined = row.findAll('td')[6].text
    ci_key = row.findAll('td')[7].text
    date_founded = row.findAll('td')[8].text
    
    tickers.append(ticker)
    names.append(name)
    sectors.append(sector)
    sub_industries.append(sub_industry)
    hq_locations.append(hq_location)
    dates_joined.append(date_joined)
    ci_keys.append(ci_key)
    dates_founded.append(date_founded)

tickers = [s.replace('\n', '').replace('.', '-') for s in tickers]
names = [s.replace('\n', '').replace('.', '-') for s in names]
sectors = [s.replace('\n', '').replace('.', '-') for s in sectors]
sub_industries = [s.replace('\n', '').replace('.', '-') for s in sub_industries]
hq_locations = [s.replace('\n', '').replace('.', '-') for s in hq_locations]
dates_joined = [s.replace('\n', '').replace('.', '-') for s in dates_joined]
ci_keys = [s.replace('\n', '').replace('.', '-') for s in ci_keys]
dates_founded = [s.replace('\n', '').replace('.', '-') for s in dates_founded]


'''
Creating S&P 500 company info csv from Wikipedia table
'''
sp500_info = pd.DataFrame(list(zip(tickers, names, sectors, sub_industries, hq_locations, dates_joined, ci_keys, dates_founded)), 
               columns = ['Symbol', 'Name', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date first added', 'CIK', 'Founded']) 
sp500_info.to_csv('S&P_500_Information.csv', index=False, header=True)


'''
Creating S&P 500 stock market data csv from Yahoo Finance
'''
start = datetime.datetime(2019,12,15)
end = datetime.datetime(2020,11,27)
data = yf.download(tickers, start=start, end=end)

data = data.unstack().reset_index()
data = data.drop(['level_0'], axis=1)

split = int(data.shape[0]/6)

adj_close = data.iloc[:split,:]

close = data.iloc[split:2*split,:]
close.index = np.arange(0, split)

high = data.iloc[2*split:3*split,:]
high.index = np.arange(0, split)

low = data.iloc[3*split:4*split,:]
low.index = np.arange(0, split)

openD = data.iloc[4*split:5*split,:]
openD.index = np.arange(0, split)

volume = data.iloc[5*split:6*split,:]
volume.index = np.arange(0, split)

final_data = close
final_data.columns = ['Symbol', 'Date', 'Close']
final_data['Daily Return'] = (close.groupby('Symbol')['Close']
                                .apply(pd.Series.pct_change))
final_data['Log Return'] = (close.groupby('Symbol')['Close']
                                .apply(lambda x: np.log(x) - np.log(x.shift(1))))

final_data = final_data[['Date', 'Symbol', 'Daily Return', 'Log Return', 'Close']]
final_data['Adj Close'] = adj_close.iloc[:,2:]
final_data['High'] = high.iloc[:,2:]
final_data['Low'] = low.iloc[:,2:]
final_data['Open'] = openD.iloc[:,2:]
final_data['Volume'] = volume.iloc[:,2:]

sp500_names = sp500_info.iloc[:,:2]
joined_data = final_data.join(sp500_names.set_index('Symbol'), on='Symbol')
joined_data = joined_data[['Date', 'Symbol', 'Name', 'Daily Return', 'Log Return', 'Close', 'Adj Close', 'High', 'Low', 'Open', 'Volume']]

joined_data.to_csv('S&P_500_stock_data.csv', index=False, header=True)


'''
Creating sectoral historical info csv
'''
tickers = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']
data = yf.download(tickers, start=start, end=end)

data = data.unstack().reset_index()
data = data.drop(['level_0'], axis=1)

split = int(data.shape[0]/6)

adj_close = data.iloc[:split,:]

close = data.iloc[split:2*split,:]
close.index = np.arange(0, split)

high = data.iloc[2*split:3*split,:]
high.index = np.arange(0, split)

low = data.iloc[3*split:4*split,:]
low.index = np.arange(0, split)

openD = data.iloc[4*split:5*split,:]
openD.index = np.arange(0, split)

volume = data.iloc[5*split:6*split,:]
volume.index = np.arange(0, split)

final_data = close
final_data.columns = ['Symbol', 'Date', 'Close']
final_data['Daily Return'] = (close.groupby('Symbol')['Close']
                                .apply(pd.Series.pct_change))
final_data['Log Return'] = (close.groupby('Symbol')['Close']
                                .apply(lambda x: np.log(x) - np.log(x.shift(1))))

final_data = final_data[['Date', 'Symbol', 'Daily Return', 'Log Return', 'Close']]
final_data['Adj Close'] = adj_close.iloc[:,2:]
final_data['High'] = high.iloc[:,2:]
final_data['Low'] = low.iloc[:,2:]
final_data['Open'] = openD.iloc[:,2:]
final_data['Volume'] = volume.iloc[:,2:]

sector_names = {'Symbol':['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY'],
                 'Name':['Materials', 'Communication Services', 'Energy', 'Financials', 'Industrials', 'Technology', 'Consumer Staples', 'Real Estate', 'Utilities', 'Health Care', 'Consumer Discretionary']}
sector_names = pd.DataFrame(sector_names)

joined_data = final_data.join(sector_names.set_index('Symbol'), on='Symbol')
joined_data = joined_data[['Date', 'Symbol', 'Name', 'Daily Return', 'Log Return', 'Close', 'Adj Close', 'High', 'Low', 'Open', 'Volume']]

joined_data.to_csv('Sectoral_stock_data.csv', index=False, header=True)


'''
Creating S&P 500 aggregate info csv
'''
tickers = ['^GSPC']
data = yf.download(tickers, start=start, end=end)

data = data.reset_index()
final_data = data
final_data['Daily Return'] = final_data['Close'].pct_change(1)
final_data['Log Return'] = np.log(final_data['Close']) - np.log(final_data['Close'].shift(1))
final_data['Symbol'] = '^GSPC'
final_data['Name'] = 'S&P 500'
final_data = final_data[['Date', 'Symbol', 'Name', 'Daily Return', 'Log Return', 'Close', 'Adj Close', 'High', 'Low', 'Open', 'Volume']]

final_data.to_csv('S&P_500_aggregate_stock_data.csv', index=False, header=True)

print('done scraping')