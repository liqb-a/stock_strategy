import pandas as pd

# 创建一个示例DataFrame
data = {'close': [100, 105, 102, 108, 110, 115, 120, 125, 130, 135, 140]}
df = pd.DataFrame(data)

# 计算10天百分比变化的绝对值
result = df['close'].pct_change(10).abs()
print(result)