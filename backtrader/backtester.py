import backtrader as bt
import pandas as pd

cerebro = bt.Cerebro()
cerebro.broker.set_cash(100000)

data = pd.read_csv('AAPL.csv', index_col='Date', parse_dates=True)
feed = bt.feeds.PandasData(dataname=data)
cerebro.adddata(feed)


print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

cerebro.run()

print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
