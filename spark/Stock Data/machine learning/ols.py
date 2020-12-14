import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import datetime as dt
import time

# reading in the data
sp500_df = pd.read_csv('../S&P_500_stock_data.csv')
sp500_agg_df = pd.read_csv('../S&P_500_aggregate_stock_data.csv')
sp500_info = pd.read_csv('../S&P_500_Information.csv')

# selecting and renaming necessary columns from original data
sp500_df = sp500_df[['Date', 'Symbol', 'Name', 'Log Return']]
sp500_info = sp500_info[['Symbol', 'GICS Sector', 'GICS Sub-Industry']]
sp500_agg_df = sp500_agg_df[['Date', 'Log Return']]

sp500_df = sp500_df.rename(columns={'Log Return':'S&P 500 Log Return'})
sp500_agg_df = sp500_agg_df.rename(columns={'Log Return':'S&P 500 Index Log Return'})

# joining the datasets
sp500_joined_train = sp500_df.join(sp500_agg_df.set_index('Date'), on='Date')
sp500_joined_train = sp500_joined_train.join(sp500_info.set_index('Symbol'), on='Symbol')
sp500_joined_train = sp500_joined_train[['Date', 'Symbol', 'Name', 'GICS Sector', 'GICS Sub-Industry', 'S&P 500 Log Return', 'S&P 500 Index Log Return']]

# filtering the data and creating training and testing sets
# train set is stock data pre-covid
# test set is stock data post-covid
filter_companies =  ['Carrier Global', 'Lumen Technologies', 'Otis Worldwide', 'ViacomCBS', 'Vontier']
sp500_joined_train = sp500_joined_train[~sp500_joined_train['Name'].isin(filter_companies)]

min_date = '2019-11-18'
max_date = '2020-02-19'

sp500_joined_train['Date'] = pd.to_datetime(sp500_joined_train['Date'])
sp500_joined_test = sp500_joined_train.copy()
sp500_joined_train = sp500_joined_train[(sp500_joined_train['Date'] >= min_date) & (sp500_joined_train['Date'] <= max_date)]
sp500_joined_test = sp500_joined_test[(sp500_joined_test['Date'] > max_date)]

# running OLS regression on training set
group_column = 'Symbol'
y_column = 'S&P 500 Log Return'
x_columns = ['S&P 500 Index Log Return']

def ols(pdf):
    group_key = pdf[group_column].iloc[0]
    y = pdf[y_column]
    X = pdf[x_columns]
    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()
    data = [tuple([group_key] + [model.params[i] for i in x_columns] + [model.params['const'], model.rsquared.round(2)] + [model.pvalues[i] for i in x_columns])]
    columns = [group_column] + x_columns + ['Constant', 'R2'] + [i + ' p value' for i in x_columns]
    return pd.DataFrame(data, columns = columns)

ols_results = sp500_joined_train.groupby('Symbol').apply(ols).reset_index(drop=True)
ols_results = ols_results.rename(columns={'S&P 500 Index Log Return':'Return Beta',
                                          'S&P 500 Index Log Return p value': 'Return p value'})

# running predictions on the test data using OLS training results
sp500_predicted = sp500_joined_test.join(ols_results.set_index('Symbol'), on='Symbol')
sp500_predicted['Predicted S&P 500 Log Return'] = sp500_predicted.apply(lambda row: row['S&P 500 Index Log Return']*row['Return Beta'] + row['Constant'], axis=1)
sp500_predicted['Actual - Predicted'] = sp500_predicted.apply(lambda row: row['S&P 500 Log Return'] - row['Predicted S&P 500 Log Return'], axis=1)

# calculating cumulative sums
sp500_predicted['S&P 500 Log Return Cumulative'] = sp500_predicted.groupby(by=['Symbol'])['S&P 500 Log Return'].cumsum()
sp500_predicted['S&P 500 Index Log Return Cumulative'] = sp500_predicted.groupby(by=['Symbol'])['S&P 500 Index Log Return'].cumsum()
sp500_predicted['Predicted S&P 500 Log Return Cumulative'] = sp500_predicted.groupby(by=['Symbol'])['Predicted S&P 500 Log Return'].cumsum()
sp500_predicted['Actual - Predicted Cumulative'] = sp500_predicted.groupby(by=['Symbol'])['Actual - Predicted'].cumsum()
sp500_predicted = sp500_predicted[['Date', 
                                    'Symbol',
                                    'Name', 
                                    'GICS Sector', 
                                    'GICS Sub-Industry',
                                    'S&P 500 Log Return',
                                    'S&P 500 Log Return Cumulative',
                                    'Predicted S&P 500 Log Return',
                                    'Predicted S&P 500 Log Return Cumulative',
                                    'Actual - Predicted',
                                    'Actual - Predicted Cumulative',
                                    'S&P 500 Index Log Return',
                                    'S&P 500 Index Log Return Cumulative',
                                    'Return Beta', 
                                    'Constant', 
                                    'R2', 
                                    'Return p value'
                                    ]]

# summing up the predictions
sp500_predicted_sum = sp500_predicted[sp500_predicted['Date'] == sp500_predicted['Date'].max()]
sp500_predicted_sum = sp500_predicted_sum.drop(columns=['Date', 
                                                        'S&P 500 Log Return', 
                                                        'S&P 500 Index Log Return', 
                                                        'Predicted S&P 500 Log Return',
                                                        'Actual - Predicted'
                                                       ])
sp500_predicted_sum = sp500_predicted_sum.rename(columns={'S&P 500 Log Return Cumulative':'S&P 500 Log Return Total',
                                                          'S&P 500 Index Log Return Cumulative': 'S&P 500 Index Log Return Total',
                                                          'Predicted S&P 500 Log Return Cumulative': 'Predicted S&P 500 Log Return Total',
                                                          'Actual - Predicted Cumulative': 'Actual - Predicted Total'
                                                         })

# filtering predictions for top volatile market covid days
volatile_days = ['2020-03-24', '2020-03-13', '2020-04-06', '2020-03-26', '2020-03-17'] + ['2020-03-16', '2020-03-12', '2020-03-09', '2020-06-11', '2020-03-18']
sp500_predicted_volatile = sp500_predicted[sp500_predicted['Date'].isin(volatile_days)]

sp500_predicted_volatile['S&P 500 Log Return Cumulative'] = sp500_predicted_volatile.groupby(by=['Symbol'])['S&P 500 Log Return'].cumsum()
sp500_predicted_volatile['S&P 500 Index Log Return Cumulative'] = sp500_predicted_volatile.groupby(by=['Symbol'])['S&P 500 Index Log Return'].cumsum()
sp500_predicted_volatile['Predicted S&P 500 Log Return Cumulative'] = sp500_predicted_volatile.groupby(by=['Symbol'])['Predicted S&P 500 Log Return'].cumsum()
sp500_predicted_volatile['Actual - Predicted Cumulative'] = sp500_predicted_volatile.groupby(by=['Symbol'])['Actual - Predicted'].cumsum()

# writing the out the data
sp500_predicted.to_csv('S&P_500_predicted_log_returns_index.csv', index=False, header=True)
sp500_predicted_sum.to_csv('S&P_500_predicted_sums_index.csv', index=False, header=True)
sp500_predicted_volatile.to_csv('S&P_500_volatile_predictions_index.csv', index=False, header=True)