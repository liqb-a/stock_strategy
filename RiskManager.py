import backtrader as bt

class RiskManager(bt.observer.Observer):
    params = (
        ('max_drawdown', 0.15),    # 最大回撤15%
        ('max_position', 0.8),     # 最大持仓50%
    )

    def __init__(self):
        self.peak = self._owner.broker.getvalue()
        self.drawdown = 0

    def next(self):
        # 计算当前回撤
        curr_val = self._owner.broker.getvalue()
        self.peak = max(self.peak, curr_val)
        self.drawdown = (self.peak - curr_val) / self.peak
        
        # 执行风控规则
        if self.drawdown >= self.p.max_drawdown:
            print(f'触发最大回撤风控：{self.drawdown*100:.1f}%')
            self._owner.close()  # 清仓
            self._owner.env.runstop()  # 停止策略
            
        # 仓位控制
        position_pct = self._owner.broker.getvalue() / self._owner.broker.startingcash
        if position_pct > self.p.max_position:
            print(f'触发仓位限制：{position_pct*100:.1f}%')
            excess = (position_pct - self.p.max_position) * self._owner.broker.startingcash
            self._owner.sell(size=excess // self._owner.data.close[0])