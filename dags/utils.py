import os, sys
import pandas as pd
import numpy as np
import yfinance as yf
from dags import config
import matplotlib.pyplot as plt
import seaborn as sns
import talib

def chose_path_to_work(PATH = ' '):
    base_folder = os.getcwd() 
    
    if PATH == ' ':
        return base_folder
    
    return base_folder + "\\" + PATH

def collect_raw_data(ticker):
    path = 'data\\raw'
    df = yf.download(ticker, period = 'max').reset_index()
    df = formating_raw_columns(df)
    ticker = str.lower(ticker)
    df.to_csv(f"{chose_path_to_work(path)}\\{ticker}.csv", index = False)

def formating_raw_columns(df):
    df.columns =  map(str.lower, df.columns)
    df.rename(columns = {'adj close': 'adj_close'}, inplace = True)
    df['date'] = pd.to_datetime(df['date'])
    return df

def plot_boxplots(df, ticker):
  
    fig, ax = plt.subplots(2, 3, figsize=(15,10))
    plt.suptitle(f'Horizontal Boxplot {ticker}')
    sns.boxplot(data=df, x='open', ax=ax[0, 0])
    ax[0, 0].set_title("Open")
    sns.boxplot(data=df, x='high', ax=ax[0, 1])
    ax[0, 1].set_title("High");
    sns.boxplot(data=df, x='low', ax=ax[0, 2])
    ax[0, 2].set_title("Low");
    sns.boxplot(data=df, x='close', ax=ax[1, 0])
    ax[1, 0].set_title("Close");
    sns.boxplot(data=df, x='adj_close', ax=ax[1, 1])
    ax[1, 1].set_title("Adj. Close");
    sns.boxplot(data=df, x='volume', ax=ax[1, 2])
    ax[1, 2].set_title("Volume");
    plt.show()
    return fig

def save_image(img, name, path = 'images/'):

    fig = img.get_figure()
    fig.savefig(f"images/{name}.png")
    print(f"Image {name} saved.")

def generate_mms(df, n_days):
    
    column_name = 'mms_' + str(n_days)
    df[column_name] = talib.SMA(df.close, n_days)
    return df

def generate_mms_label(df, mms_short_collumn = 'mms_15', mms_long_column = 'mms_30'):
    
    df['mms_sinal'] = np.where(df[mms_short_collumn] > df[mms_long_column], 1, 0)
    df['mms_cruza'] = df['mms_sinal'].diff()
    df['mms_compra'] = np.where(df['mms_cruza'] == 1, 1, 0)
    df['mms_venda'] = np.where(df['mms_cruza'] == -1, 1, 0)
    df.drop(columns= ['mms_sinal', 'mms_cruza'], inplace = True)
    return df

def generate_mme(df, n_days):
    
    column_name = 'mme_' + str(n_days)
    df[column_name]= talib.EMA(df.close, timeperiod= n_days)
    return df

def generate_mme_label(df, adj_upper, adj_lower, n_days):
    
    column_name = 'mme_' + str(n_days)

    lower_name =  column_name + '_adj_' + str(int(adj_lower * 100))
    upper_name =  column_name + '_adj_' + str(int(adj_upper * 100))
    
    df['mme_temp']= talib.EMA(df.close, timeperiod= n_days)
    df[upper_name] = df['mme_temp'] * adj_upper
    df[lower_name] = df['mme_temp'] * adj_lower
    df['prox_upper'] = abs(df.close - df[upper_name]) 
    df['prox_lower'] = abs(df.close - df[lower_name])
    df['prox_mme_adj'] = df['prox_lower'] - df['prox_upper'] 
    df.drop(columns = ['mme_temp', 'prox_upper', 'prox_lower'], inplace = True)
    return df

def generate_bollinger_bands(df, n_days = 20, nbdevup=2, nbdevdn=2, matype=0, flag = True):

    df['bb_upper'], df['bb_central'], df['bb_lower'] = talib.BBANDS(df.close, timeperiod = n_days, nbdevup = nbdevup, nbdevdn = nbdevdn, matype = matype)
    df['bb_band_dist'] = (df['close'].rolling(20).std())*4
    
    if (flag == True):    
        df['bb_sobrecomprada'] = np.where(df['close'] > df['bb_upper'], 1, 0)
        df['bb_sobrevendida'] = np.where(df['close'] < df['bb_lower'], 1, 0)
    
    return df

def generate_ifr(df, n_days = 14, upper_limit = 70, lower_limit = 30, flag = True):

    df['ifr'] = talib.RSI(df.close, timeperiod=n_days)
    
    if (flag == True):    
        df['ifr_sobrecomprada'] = np.where(df['ifr'] >= upper_limit, 1, 0)
        df['ifr_sobrevendida'] = np.where(df['ifr'] <= lower_limit, 1, 0)
    
    return df

def generate_macd(df, fastperiod=12, slowperiod=26, signalperiod=9, flag = True):

    df['macd'], df['macd_sinal'], df['macd_hist'] = talib.MACD(df.close, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    
    if (flag == True):    
        df['macd_acima'] = np.where(df['macd'] > df['macd_sinal'], 1, 0)
        df['macd_cruza'] = df['macd_acima'].diff()
        df['macd_alta'] = np.where(df['macd_cruza'] == 1, 1, 0)
        df['macd_baixa'] = np.where(df['macd_cruza'] == -1, 1, 0)
        df.drop(columns= ['macd_acima', 'macd_cruza'], inplace = True)
            
    return df

def generate_label(n_days, df):
    df = df.copy()
    n_days = n_days * -1
    df['close_shift'] = df['close'].shift(n_days)
    df['target'] = np.where((df['close_shift'] - df['close']) >= 0, 1, 0)
    df.dropna(inplace = True)
    df.drop(columns = ['close_shift'], inplace = True)
    return df