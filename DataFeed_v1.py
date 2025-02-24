import akshare as ak
import pandas as pd
import backtrader as bt
import datetime
import os
import time
import multiprocessing
from tqdm import tqdm
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

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
    def fetch_data(cls, symbol, start_date, end_date, retries=3):
        """获取数据并增加重试机制"""
        for i in range(retries):
            try:
                df = ak.stock_zh_a_daily(
                    symbol=symbol, adjust="qfq", 
                    start_date=start_date, end_date=end_date, 
                )
                df.rename(columns={'日期':'date','开盘':'open','最高':'high',
                                '最低':'low','收盘':'close','成交量':'volume'}, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                logging.info(f"Successfully fetched data for {symbol}")
                return df
            except Exception as e:
                logging.warning(f"Error fetching data for {symbol} (attempt {i+1}/{retries}): {e}")
                time.sleep(2)
        logging.error(f"Failed to fetch data for {symbol} after {retries} attempts")
        return pd.DataFrame()
    
    @classmethod
    def save_to_db(cls, symbol, df, conn):
        """将数据保存到数据库"""
        try:
            # 检查是否已存在相同日期的数据
            existing_dates = pd.read_sql(f'SELECT date FROM {symbol}', conn)['date']
            df = df[~df.index.isin(existing_dates)]  # 过滤掉已存在的数据
        except Exception as e:
            logging.warning(f'error select data for {symbol}: {e}')
        finally:
            if not df.empty:
                df.to_sql(symbol, conn, if_exists='append', index=True)

    @classmethod
    def fetch_all_data(cls, start_date, end_date):
        """获取所有股票数据"""
        code_list = cls.fetch_code_list()
        num_threads = min(multiprocessing.cpu_count() * 2, len(code_list))  # 动态调整线程数
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {executor.submit(cls.process_symbol, code, start_date, end_date): code for code in code_list}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching data"):
                code = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing {code}: {e}")
        return code_list

    @classmethod
    def process_symbol(cls, symbol, start_date, end_date):
        """处理单个股票的数据"""
        conn = sqlite3.connect('./data/stock_data.db')  # 每个线程创建独立的连接
        try:
            df = cls.fetch_data(symbol, start_date, end_date)
            if not df.empty:
                cls.save_to_db(symbol, df, conn)
        except Exception as e:
            logging.error(f"Error processing {symbol}: {e}")
        finally:
            conn.close() #关闭连接

    @classmethod
    def fetch_code_list(cls):
        """获取股票代码列表并缓存到本地"""
        cache_file = './data/stock_code_list.csv'
        if os.path.exists(cache_file):
            stock_list = pd.read_csv(cache_file)
        else:
            stock_list = ak.stock_zh_a_spot()
            stock_list.to_csv(cache_file, index=False)
        
        # 过滤ST股票、科创板、创业板
        stock_list = stock_list[~stock_list['名称'].str.contains('ST')]
        stock_list = stock_list[~stock_list['代码'].str.contains('sh688')]
        stock_list = stock_list[~stock_list['代码'].str.contains('sz300')]
        
        return stock_list['代码'].tolist()

if __name__ == '__main__':
    start_date = '20250101'
    now = datetime.datetime.now()
    end_date = now.strftime('%Y%m%d')
    df = UniversalDataFeed.fetch_all_data(start_date, end_date)