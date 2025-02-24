import akshare as ak
import pandas as pd
import backtrader as bt
import datetime
import os
from tqdm import tqdm
import sqlite3

class UniversalDataFeed(bt.feeds.PandasData):
    params = (
        ('datetime', None),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', -1)
    )
    
    @classmethod
    def fetch_data(cls, symbol, start_date, end_date):
        """获取并预处理数据"""
        try:
            df = ak.stock_zh_a_daily(
            symbol=symbol, adjust="qfq", 
            start_date=start_date, end_date=end_date, 
            )
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
        df.rename(columns={'日期':'date','开盘':'open','最高':'high',
                          '最低':'low','收盘':'close','成交量':'volume'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df
    
    @classmethod
    def fetch_all_data(cls, start_date, end_date):
        """获取所有股票数据"""
        code_list = cls.fetch_code_list()
        for code in tqdm(code_list, desc="Fetching data"):
            # save to csv
            if f'{code}.csv' not in os.listdir():
                print(f"Fetching data for {code}")
                cls.save_history_data(code, start_date, end_date)
            else:
                print(f"Adding new data for {code}")
                cls.add_new_data(code, start_date, end_date)
        return code_list
    
    @classmethod
    def fetch_code_list(cls):
        """获取股票代码列表"""
        stock_list = ak.stock_zh_a_spot()
        # 过滤ST股票
        stock_list = stock_list[~stock_list['名称'].str.contains('ST')]
        # 过滤科创板股票（代码以688开头）and 创业板股票（代码以300开头）
        stock_list = stock_list[~stock_list['代码'].str.contains('sh688')]
        stock_list = stock_list[~stock_list['代码'].str.contains('sz300')]
    
        print(len(stock_list['代码'].tolist()))
        return stock_list['代码'].tolist()
    
    @classmethod
    def save_history_data(cls, symbol, start_date, end_date):
        """保存历史数据到数据库"""
        df = cls.fetch_data(symbol, start_date, end_date)
        # 判断df内容是否为空
        if not df.empty:
            cls.save_to_db(symbol, df)
    
    @classmethod
    def save_to_db(cls, symbol, df):
        """将数据保存到数据库"""
        conn = sqlite3.connect('stock_data.db')
        # 将数据保存到数据库 add new data
        df.to_sql(symbol, conn, if_exists='append', index=True)
        conn.close()
    @classmethod
    def add_new_data(cls, symbol, start_date, end_date):
        """添加新数据"""
        df_old = pd.read_csv(f'{symbol}.csv', index_col='date', parse_dates=True)
        start_date = df_old.index.max() + datetime.timedelta(days=1)
        if start_date > pd.to_datetime(end_date):
            print(f'{symbol} data is already up to date.')  # 无需更新
            return
        df = cls.fetch_data(symbol, start_date, end_date)
        if  not df.empty:
            # append new data to the old data
            df.to_csv(f'{symbol}.csv', mode='a', header=False)

if __name__ == '__main__':
    start_date = '20250101'
    now = datetime.datetime.now()
    end_date = now.strftime('%Y%m%d')
    df = UniversalDataFeed.fetch_all_data(start_date, end_date)
    
    '''# 获取某只股票的历史数据（如贵州茅台，股票代码 600519）
    # UniversalDataFeed.save_history_data('sh600519', '20220101', '20220131')
    # UniversalDataFeed.fetch_code_list()'''
    
