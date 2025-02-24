import unittest
import backtrader as bt
from Strategy import MultiStrategy
import datetime
from DataFeed import UniversalDataFeed
import pandas as pd
# import talib
import os
from tqdm import tqdm

class TestMultiStrategy(unittest.TestCase):

    def setUp(self):
        self.cerebro = bt.Cerebro()
        # self.data = bt.feeds.YahooFinanceData(dataname='AAPL', fromdate=datetime.datetime(2020, 1, 1), todate=datetime.datetime(2021, 1, 1))
        # read csv file csv file is in the format of 'date,股票代码,open,close,high,low,volume,成交额,振幅,涨跌幅,涨跌额,换手率'
        self.data = bt.feeds.GenericCSVData(dataname='./data/000001.csv', dtformat=('%Y-%m-%d'), datetime=0, open=2, high=4, low=5, close=3, volume=6, openinterest=-1)
        # 可视化 data 数据，确认数据加载是否正确
        print('Data Loaded: %d' % len(self.data))
        for line in self.data:
            print(line.datetime.date(), line.open, line.high, line.low, line.close, line.volume)
        
        self.cerebro.adddata(self.data)
        self.cerebro.addstrategy(MultiStrategy)
    '''
    def test_strategy_initialization(self):
        self.cerebro.run()
        strategy = self.cerebro.strategies[0]
        self.assertIsNotNone(strategy.bbi)
        self.assertIsNotNone(strategy.macd)
        self.assertIsNotNone(strategy.rsi)
        self.assertIsNotNone(strategy.resistance)
        self.assertIsNotNone(strategy.ma60)
        self.assertIsNotNone(strategy.vol_ma5)
        self.assertIsNotNone(strategy.drawdown)
    
    def test_generate_bbi_signal(self):
        self.cerebro.run()
        strategy = self.cerebro.strategies[0]
        signal = strategy.generate_bbi_signal()
        self.assertIn(signal, [-1, 0, 1])

    def test_generate_pm_signal(self):
        self.cerebro.run()
        strategy = self.cerebro.strategies[0]
        signal = strategy.generate_pm_signal()
        self.assertIn(signal, [-1, 0, 1])

    def test_generate_gp_signal(self):
        self.cerebro.run()
        strategy = self.cerebro.strategies[0]
        signal = strategy.generate_gp_signal()
        self.assertIn(signal, [-1, 0, 1])

    # def test_execute_trade(self):
    #     self.cerebro.run()
    #     strategy = self.cerebro.strategies[0]
    #     initial_cash = strategy.broker.get_cash()
    #     strategy.final_signal = 1
    #     strategy.execute_trade()
    #     self.assertNotEqual(strategy.broker.get_cash(), initial_cash)'''
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
    df.loc[pit_condition & recovery_condition, 'signal'] = 1  # 延迟2日确认
    # df.loc[pit_condition, 'signal'] = 1  
    # df.loc[recovery_condition, 'signal'] = 1
    df.loc[df['close'] < 0.97 * df['close'].rolling(10).min(), 'signal'] = -1
    return df


#################自定义策略
def pit_up_strategy(df, ma_period=60, drop_threshold=0.15):
    # 计算技术指标
    df['MA60'] = df['close'].rolling(ma_period).mean()
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
    
    # 检测新的买入信号条件
    buy_condition = (
        (df['close'] < df['open']) &  # 当日收阴
        (df['close'].shift(1) > df['open'].shift(1)) &  # 前日收阳
        (df['volume'] < df['volume'].shift(1)) &  # 较比上一日缩量
        (df['upper_shadow'] > df['lower_shadow'])  # 当日上影线长于下影线
    )
    
    # 综合信号
    df['signal'] = 0
    df.loc[buy_condition, 'signal'] = 1  # 买入信号
    df.loc[df['close'] < 0.97 * df['close'].rolling(10).min(), 'signal'] = -1  # 卖出信号
    
    return df



if __name__ == '__main__':
    
    '''# BBI示例调用
    df = calculate_bbi(df)
    df = generate_bbi_signals(df)
    print(df[['close', 'BBI', 'signal']].tail())'''
    
    # # 价格动量示例调用
    # df = golden_pit_strategy(df)
    # print(df[['close', 'MA60', 'signal']].tail())
    # 遍历data文件夹下的所有股票数据
    for filename in os.listdir('./data'):
        print(filename)
        try:
            df = pd.read_csv('./data/' + filename, index_col='date', parse_dates=True)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue
        df = pit_up_strategy(df)
        if df['signal'].iloc[-1] == 1:
            print(f"pit_pu: {filename}: {df[['close', 'MA60', 'signal']].tail(1)}")
        df = golden_pit_strategy(df)
        if df['signal'].iloc[-1] == 1:
            print(f"golden_pit: {filename}: {df[['close', 'MA60', 'signal']].tail(1)}")
    # df = pd.read_csv('./data/sh600589.csv', index_col='date', parse_dates=True)
    # # 打印末尾几行数据
    # print(df.tail())
    # df = golden_pit_strategy(df)
    # print(df[['close', 'MA60', 'signal']].tail())
    # # 打印signal为1的日期
    # print(df[df['signal'] != 0][['close', 'MA60', 'signal']])
    
    