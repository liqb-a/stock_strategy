import unittest
import backtrader as bt
from Strategy import MultiStrategy
import datetime
from DataFeed import UniversalDataFeed
import pandas as pd
# import talib
import os
from tqdm import tqdm
import sqlite3
import os

################# 生成BBI指标
# 计算BBI指标
def calculate_bbi(df):
    df['MA3'] = df['close'].rolling(3).mean()
    df['MA6'] = df['close'].rolling(6).mean()
    df['MA12'] = df['close'].rolling(12).mean()
    df['MA24'] = df['close'].rolling(24).mean()
    df['BBI'] = (df['MA3'] + df['MA6'] + df['MA12'] + df['MA24']) / 4
    return df
# 生成交易信号
def generate_bbi_signals(df):
    df['signal'] = 0
    # 买入：收盘价上穿BBI且MACD金叉
    df.loc[(df['close'] > df['BBI']) & (df['close'].shift(1) <= df['BBI'].shift(1)), 'signal'] = 1
    # 卖出：收盘价下穿BBI
    df.loc[(df['close'] < df['BBI']) & (df['close'].shift(1) >= df['BBI'].shift(1)), 'signal'] = -1
    return df

'''########################### 生成价格动量策略信号
def price_momentum_strategy(df):
    # 计算指标
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    df['MA5'] = df['close'].rolling(5).mean()
    
    # 自动识别近期阻力位（过去20日最高点）
    df['resistance'] = df['high'].rolling(20).max()
    
    # 生成信号
    df['signal'] = 0
    # 买入：突破阻力位且RSI<70
    df.loc[
        (df['close'] > df['resistance'].shift(1)) & 
        (df['volume'] > 1.5 * df['volume'].rolling(5).mean()) & 
        (df['RSI'] < 70), 'signal'] = 1
    
    # 卖出：RSI超买或跌破MA5
    df.loc[(df['RSI'] > 80) | (df['close'] < df['MA5']), 'signal'] = -1
    return df
    '''

########################### 生成黄金坑策略信号
def golden_pit_strategy(df, ma_period=60, drop_threshold=0.15):
    df['MA60'] = df['close'].rolling(ma_period).mean()
    df['Vol_MA5'] = df['volume'].rolling(5).mean()
    
    # 检测缩量挖坑
    df['drawdown'] = df['close'].pct_change(10).abs()
    pit_condition = (
        (df['drawdown'] >= drop_threshold) &
        (df['volume'] < 0.5 * df['Vol_MA5'])
    )
    
    # 检测放量反包
    recovery_condition = (
        (df['close'] > df['open']) & 
        (df['volume'] > 2 * df['volume'].shift(1)) & 
        (df['close'] > df['MA60'])
    )
    
    # 综合信号
    df['signal'] = 0
    df.loc[pit_condition.shift(8) & recovery_condition, 'signal'] = 1  # 延迟2日确认
    # df.loc[pit_condition, 'signal'] = 1  
    # df.loc[recovery_condition, 'signal'] = 1
    df.loc[df['close'] < 0.97 * df['close'].rolling(10).min(), 'signal'] = -1
    return df


#################自定义策略
def pit_up_strategy(df, ma_period=30, drop_threshold=0.15):
    # 计算技术指标
    df['MA30'] = df['close'].rolling(ma_period).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA5'] = df['close'].rolling(5).mean()

    df['Vol_MA5'] = df['volume'].rolling(5).mean()
    
    # 检测缩量挖坑
    df['drawdown'] = df['close'].pct_change(10).abs()
    pit_condition = (
        (df['drawdown'] >= drop_threshold) &
        (df['volume'] < 0.5 * df['Vol_MA5'])
    )
    
    # 计算上影线和下影线长度
    df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)  # 上影线 = 最高价 - max(开盘价, 收盘价)
    df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']   # 下影线 = min(开盘价, 收盘价) - 最低价
    # 当日实体阴线或阳线长度
    df['body'] = abs(df['open'] - df['close'])
    
    # 检测新的买入信号条件
    buy_condition = (
        (df['close'] < df['open']) &  # 当日收阴
        (df['close'].shift(1) > df['open'].shift(1)) &  # 前日收阳
        (df['volume'] < df['volume'].shift(1)) &  # 较比上一日缩量
        (df['upper_shadow'] > df['lower_shadow']) &  # 当日上影线长于下影线
        (df['body'] < df['upper_shadow']) & # 当日实体部分小于上影线
        ~((df['close'] > df['MA30']) | (df['close'] > df['MA20']) | (df['close'] > df['MA10']) | (df['close'] > df['MA5']))   # 收盘价在MA30之上
    )
    
    # 综合信号
    df['signal'] = 0
    df.loc[buy_condition, 'signal'] = 1  # 买入信号
    df.loc[df['close'] < 0.97 * df['close'].rolling(10).min(), 'signal'] = -1  # 卖出信号
    
    return df

def fetch_data_from_db(symbol, conn):
    """从数据库中读取数据"""
    query = f"SELECT * FROM {symbol}"
    try:
        df = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])
        return df
    except Exception as e:
        print(f"Error reading data for {symbol}: {e}")
        return pd.DataFrame()

def main():
    # 连接到数据库
    conn = sqlite3.connect('./data/stock_data.db')
    
    # 获取所有股票代码
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # 遍历所有表（股票代码）
    for table in tables:
        symbol = table[0]  # 表名即股票代码
        # print(f"Processing {symbol}")
        
        try:
            # 从数据库中读取数据
            df = fetch_data_from_db(symbol, conn)
            if df.empty:
                continue
            
            # 应用策略
            # df = pit_up_strategy(df)
            # if df['signal'].iloc[-1] == 1:
            #     print(f"pit_pu: {symbol}: {df[['close', 'MA10', 'signal']].tail(1)}")
            
            df = golden_pit_strategy(df)
            if df['signal'].iloc[-1] == 1:
                print(f"golden_pit: {symbol}: {df[['close', 'MA60', 'signal']].tail(1)}")
        
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    # 关闭数据库连接
    conn.close()


if __name__ == '__main__':
    main()
    
    '''# BBI示例调用
    df = calculate_bbi(df)
    df = generate_bbi_signals(df)
    print(df[['close', 'BBI', 'signal']].tail())'''
    
    # # 价格动量示例调用
    # df = golden_pit_strategy(df)
    # print(df[['close', 'MA60', 'signal']].tail())
    # 遍历data文件夹下的所有股票数据
    # for filename in os.listdir('./data'):
    #     print(filename)
    #     try:
    #         df = pd.read_csv('./data/' + filename, index_col='date', parse_dates=True)
    #     except Exception as e:
    #         print(f"Error reading {filename}: {e}")
    #         continue
    #     df = pit_up_strategy(df)
    #     if df['signal'].iloc[-1] == 1:
    #         print(f"pit_pu: {filename}: {df[['close', 'MA60', 'signal']].tail(1)}")
    #     df = golden_pit_strategy(df)
    #     if df['signal'].iloc[-1] == 1:
    #         print(f"golden_pit: {filename}: {df[['close', 'MA60', 'signal']].tail(1)}")
    # df = pd.read_csv('./data/sh600589.csv', index_col='date', parse_dates=True)
    # # 打印末尾几行数据
    # print(df.tail())
    # df = golden_pit_strategy(df)
    # print(df[['close', 'MA60', 'signal']].tail())
    # # 打印signal为1的日期
    # print(df[df['signal'] != 0][['close', 'MA60', 'signal']])
    
    