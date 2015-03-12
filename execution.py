import datetime
import Queue
import time
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from event import FillEvent, OrderEvent
from data import HistoricCSVDataHandler
import os
import time
from strategy import PivotMA

class ExecutionHandler(object):
    """
    The ExecutionHandler abstract class handles the interaction
    between a set of order objects generated by a Portfolio and
    the ultimate set of Fill objects that actually occur in the
    market.

    The handlers can be used to subclass simulated brokerages
    or live brokerages, with identical interfaces. This allows
    strategies to be backtested in a very similar manner to the
    live trading engine.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_order(self, event):
        """
        Takes an Order event and executes it, producing
        a Fill event that gets placed onto the Events queue.

        Parameters:
        event - Contains an Event object with order information.
        """
        raise NotImplementedError("Should implement execute_order")


class SimulatedExecutionHandler(ExecutionHandler):
    """
    The simulated execution handler simply converts all order
    objects into their equivalent fill objects automatically
    without latency, slippage or fill-ratio issues.

    This allows a straightforward "first go" test of any strategy,
    before implementation with a more sophisticated execution
    handler.
    """
    def __init__(self,events):
        self.events = events

    def execute_order(self,event):
        """
        Simply converts Order objects into Fill objects naively,
        i.e. without any latency, slippage or fill ratio problems.

        Parameters:
        event - Contains an Event object with order information.
        """
        if event.type == 'ORDER':
            fill_event = FillEvent(datetime.datetime.utcnow(), event.symbol, 'ARCA', event.quantity, event.direction, None)
            self.events.put(fill_event)





if __name__ == '__main__':

    from strategy import BuyAndHoldStrategy
    from event import *
    from portfolio import ClainPortfolio
    import socket
    events=Queue.Queue()
    #q.put(MarketEvent())
    symbol = 'USDEUR'
    laptop_dir='/home/dimitris/gitProjects/fxtrade/data'
    desktop_dir = '/home/dimitris/workspace/python/PycharmProjects/quant/data'
    disou_path='C:\Users\Dimitris\PycharmProjects\quant\data'
    if socket.gethostname()=='laptop':
        bars = HistoricCSVDataHandler(events, laptop_dir, [f[:-4] for f in os.listdir(laptop_dir) if f.endswith('.csv')])
    elif socket.gethostname()=='desktop':
        bars = HistoricCSVDataHandler(events, desktop_dir, [f[:-4] for f in os.listdir(desktop_dir) if f.endswith('.csv')])
    elif socket.gethostname()=='Disou_pc':
        bars=HistoricCSVDataHandler(events,disou_path, [f[:-4] for f in os.listdir(disou_path) if f.endswith('.csv')])

    strategy = BuyAndHoldStrategy(bars,events)
    #strategy = PivotMA(bars,events,plot=False)
    port = ClainPortfolio(bars,events, '20010103 00:00:00',initial_capital=10000)   #'2014.05.01 00:00')

    broker=SimulatedExecutionHandler(events)
    start_time=time.time()



    # for i in range(5):
    #     bars.update_bars()
    # #    bars.update_bars()
#    print bars.get_latest_bars(symbol)
#     while True:
#         bars.update_bars()
#         #print bars.get_latest_bars('USDEUR')
#         time.sleep(1)
    while True:
    # Update the bars (specific backtest code, as opposed to live trading)
        if bars.continue_backtest == True:
            bars.update_bars()
            #print bars.get_latest_bars("USDEUR",N=1)
        else:
            break

    # Handle the events
        while True:
            try:
                event = events.get(False)
            except Queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == 'MARKET':
                        #print 'MARKET'
                        strategy.calculate_signals(event)

                        #print bars.get_latest_bars('USDEUR')

                    elif event.type == 'SIGNAL':

                        port.update_signal(event)
                        #port.update_timeindex(event)

                        #print 'SINGAL'
                    elif event.type == 'ORDER':
                        broker.execute_order(event)
                       # port.update_stats()
                    elif event.type == 'FILL':
                        port.update_fill(event)


    df= pd.DataFrame(port.final_order_boook)
    df['Cumulative_PnL']=df['PnL'].cumsum()
    df['Return']=df['Total_equity'].pct_change()
    total_trades=df['PnL'].count()
    PercentagePositive= df['PnL'][df['PnL']>=0].count()/float(df['PnL'].count())
    PercentageNegative= df['PnL'][df['PnL']<0].count()/float(df['PnL'].count())
    AveragePositive=df['PnL'][df['PnL']>=0].mean()
    AverageNegative=df['PnL'][df['PnL']<0].mean()
    MaxPositive=df['PnL'][df['PnL']>=0].max()
    MaxNegative=df['PnL'][df['PnL']<0].min()
    GrossWins = df['PnL'][df['PnL']>=0].sum()
    GrossLosses = df['PnL'][df['PnL']<0].sum()
    Expectancy=(PercentagePositive*AveragePositive)+(PercentageNegative*AverageNegative)
    print 'Total Trades:',total_trades
    print np.sqrt(252) * (df['Return'].mean()) / df['Return'].std()
    #print 'PercentPositive = %.4f', 'PercentNegative = %.4f', 'AveragePositive = %.4f', 'AverageNegative = %.4f' %(PercentagePositive,PercentagePositive,AveragePositive,AverageNegative)
    print (PercentagePositive,PercentageNegative,AveragePositive,AverageNegative)
    #print 'MaxPositive = %.4f', 'MaxNegative = %.4f', 'Expectancy = %.4f' %(MaxPositive,MaxNegative,Expectancy)
    print (MaxPositive,MaxNegative,GrossWins,GrossLosses,Expectancy)
    elapsed_time=time.time()-start_time
    print "\nElapsed Time", elapsed_time,'sec, ', '[',elapsed_time/60,' min] '
    df.index=pd.to_datetime(df['Date'])
    grouped=df.groupby(df.index.year)
    print grouped['PnL'].sum()/grouped['PnL'].count()
    df['Total_equity'].plot()
    plt.show()
    plt.close()


    #df.to_csv('Tirinini.csv')
    #10-Minute heartbeat
     #   time.sleep(.52)