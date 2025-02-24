import backtrader as bt
from Strategy import MultiStrategy
from RiskManager import RiskManager
from DataFeed import UniversalDataFeed

def run_backtest(symbol='sh600612', start='2020-01-01', end='2025-02-18'):
    cerebro = bt.Cerebro()
    
    # 加载数据
    df = UniversalDataFeed.fetch_data(symbol, start, end)
    print(df.head())
    if df.empty:
        raise ValueError("Fetched data is empty. Please check the data source or the date range.")
    data = UniversalDataFeed(dataname=df)
    cerebro.adddata(data)
    
    # 添加策略
    cerebro.addstrategy(MultiStrategy)
    
    # 添加风控
    # cerebro.addobserver(RiskManager)
    
    # 设置初始资金和佣金
    cerebro.broker.setcash(35000)
    cerebro.broker.setcommission(commission=0.001, margin=0)
    
    # 运行回测
    print('初始资金: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('最终资金: %.2f' % cerebro.broker.getvalue())
    
    # 可视化
    cerebro.plot(style='candlestick', volume=False)

if __name__ == '__main__':
    run_backtest()