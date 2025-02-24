import backtrader as bt

class MultiStrategy(bt.Strategy):
    params = (
        # BBI策略参数
        ('bbi_periods', [3, 6, 12, 24]),
        # 价格动量策略参数
        ('rsi_period', 14),
        ('resistance_window', 20),
        # 黄金坑策略参数
        ('pit_ma_period', 60),
        ('pit_drop_threshold', 0.15),
    )

    def __init__(self):
        # 数据引用
        self.dataclose = self.datas[0].close
        self.volume = self.datas[0].volume
        
        # 初始化各策略指标
        self.init_bbi()
        self.init_price_momentum()
        self.init_golden_pit()
        
        # 信号记录
        self.signal_bbi = 0
        self.signal_pm = 0
        self.signal_gp = 0
        self.final_signal = 0

    def init_bbi(self):
        """BBI指标计算"""
        ma3 = bt.indicators.SMA(self.data.close, period=self.p.bbi_periods[0])
        ma6 = bt.indicators.SMA(self.data.close, period=self.p.bbi_periods[1])
        ma12 = bt.indicators.SMA(self.data.close, period=self.p.bbi_periods[2])
        ma24 = bt.indicators.SMA(self.data.close, period=self.p.bbi_periods[3])
        self.bbi = (ma3 + ma6 + ma12 + ma24) / 4
        self.macd = bt.indicators.MACD(self.data.close)

    def init_price_momentum(self):
        """价格动量指标"""
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)
        self.resistance = bt.indicators.Highest(self.data.high(-1), 
                                            period=self.p.resistance_window)

    def init_golden_pit(self):
        """黄金坑指标"""
        self.ma60 = bt.indicators.SMA(self.data.close, period=self.p.pit_ma_period)
        self.vol_ma5 = bt.indicators.SMA(self.data.volume, period=5)
        self.drawdown = self.data.close / bt.indicators.Highest(self.data.close, period=10) - 1

    def next(self):
        # 生成各策略信号
        self.signal_bbi = self.generate_bbi_signal()
        self.signal_pm = self.generate_pm_signal()
        self.signal_gp = self.generate_gp_signal()
        
        # 信号融合（投票机制）
        vote = self.signal_bbi + self.signal_pm + self.signal_gp
        if vote >= 2:   # 至少两个策略给出买入信号
            self.final_signal = 1
        elif vote <= -2:
            self.final_signal = -1
        else:
            self.final_signal = 0
            
        # 执行交易
        self.execute_trade()

    def generate_bbi_signal(self):
        """BBI策略信号"""
        if self.data.close[0] > self.bbi[0] and \
           self.data.close[-1] <= self.bbi[-1] and \
           self.macd.macd[0] > self.macd.signal[0]:
            return 1
        elif self.data.close[0] < self.bbi[0] and \
             self.data.close[-1] >= self.bbi[-1]:
            return -1
        return 0

    def generate_pm_signal(self):
        """价格动量策略信号"""
        if self.data.close[0] > self.resistance[0] and \
           self.volume[0] > 1.5 * self.vol_ma5[0] and \
           self.rsi[0] < 70:
            return 1
        elif self.rsi[0] > 80 or \
             self.data.close[0] < bt.indicators.SMA(self.data.close, period=5)[0]:
            return -1
        return 0

    def generate_gp_signal(self):
        """黄金坑策略信号"""
        cond1 = (abs(self.drawdown[0]) >= self.p.pit_drop_threshold) and \
                (self.volume[0] < 0.5 * self.vol_ma5[0])
        cond2 = (self.data.close[0] > self.data.open[0]) and \
                (self.volume[0] > 2 * self.volume[-1]) and \
                (self.data.close[0] > self.ma60[0])
        if cond1 and cond2:
            return 1
        elif self.data.close[0] < 0.97 * bt.indicators.Lowest(self.data.close, period=10)[0]:
            return -1
        return 0

    def execute_trade(self):
        """执行交易指令"""
        if self.final_signal == 1 and not self.position:
            size = self.broker.getvalue() * 0.2 // self.data.close[0]  # 20%仓位
            self.buy(size=size)
        elif self.final_signal == -1 and self.position:
            self.sell(size=self.position.size)