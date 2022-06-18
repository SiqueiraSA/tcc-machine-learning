import os, sys
import pandas as pd
import yfinance as yf
from dags import config
import matplotlib.pyplot as plt
import seaborn as sns

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