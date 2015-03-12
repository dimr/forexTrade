import datetime
import numpy as np
import pandas as pd
import Queue
import time
from abc import ABCMeta, abstractmethod
from math import floor
import copy

from event import FillEvent, OrderEvent
from collections import namedtuple, OrderedDict


class orderBookEntry(object):
    def __init__(self):
        self._mkt_quantity = None
        self._direction = None
        self._orderType = None
        self._price = None
        self._side = None
        self._date=None

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, date):
        self._date = date

    @property
    def mkt_quantity(self):
        return self._mkt_quantity

    @mkt_quantity.setter
    def mkt_quantity(self, q):
        self._mkt_quantity = q

    @property
    def direction(self):
        return self._direction

    @direction.setter
    def direction(self, d):
        self._direction = d

    @property
    def orderType(self):
        return self._orderType

    @orderType.setter
    def orderType(self, o):
        self._orderType = o

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, price):
        self._price = price

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, side):
        self._side = side


    def __str__(self):
        return "Date:" + str(self._date) + " Quantity:" + str(self._mkt_quantity) + " Direction:" + str(
            self._direction) + " Price:" + str(self._price) + " Side:" + str(self._side)


class TradeStats(object):
    def __init__(self):
        # assert(isinstance(enter_position,orderBookEntry) and isinstance(exit_position,orderBookEntry))
        # assert(enter_position.side == 'LONG' or enter_position.side == 'SHORT')
        # assert(enter_position.direction == 'EXIT')
        self._enter_position = None
        self._exit_position = None
        self._position = None
        self._pnl=None
        self._money_earned=None
        self._total_equity=None

    @property
    def position(self):
        return self._position

    @position.setter
    def position(self, position):
        if position.side == 'LONG' or position.side == 'SHORT':
            self._enter_position = position
        if position.side == 'EXIT':
            self._exit_position = position

    @property
    def enter_position(self):
        return self._enter_position

    @enter_position.setter
    def enter_position(self, position):
        assert(position.side == 'SHORT' or position.side=='LONG')
        self._enter_position = position

    @property
    def exit_position(self):
        return self._exit_position

    @exit_position.setter
    def exit_position(self, position):
        assert(position.side=='EXIT')
        #print '->>>>>>>>>>>>>',self.enter_position
        self._exit_position = position

    @property
    def npl(self):
        return self._pnl


    def pnl(self):
        if self.enter_position.side=='SHORT':
            return (self.enter_position.price - self.exit_position.price)-0.0004
        elif self.enter_position.side=='LONG':
            return  (self.exit_position.price-self.enter_position.price)-0.0004

    @property
    def money_earned(self):
        return self._money_earned

    def money_earned(self):
        return self.pnl()*abs(self.enter_position.mkt_quantity)

    # @property
    # def total_equity(self):
    #     return self._total_equity


    def __str__(self):
        return "**ENTER:" + str(self._enter_position) + " **EXIT:" + str(self._exit_position)+"\n"+" npl:"+str(self.pnl())+" Money Earned:"+str(self.money_earned())\
        +' Cumulative PNL:'+str(self.cumulative_pnl)+" Total Equity:"+str(self.total_equity)+" Percentage Equity Change:"+str(self.pct_equity_change)


class Portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        raise NotImplementedError("Should implement update_signal()")


    @abstractmethod
    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        raise NotImplementedError("Should implement update_fill()")


class ClainPortfolio(Portfolio):
    """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        (USD unless otherwise stated).

        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """

    def __init__(self, bars, events, start_date, initial_capital=1000.0):
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = None
        self.initial_capital = initial_capital


        # instance variables
        self.pip_stop = 35
        self.risk = 0.05
        self.mkt_quantity=self._get_market_quantity()
        self.bookEntry = orderBookEntry()
        self.bookEntry.mkt_quantity=0
        self.order_book = OrderedDict()
        self.counter = 0
        self.side = ''
        self.trade_stats = []
        #self.trade_stats.append(datetime.datetime.now())
        self.trades = TradeStats()

        self.temp_enter_point=None

        #----------- CUMULATIVES -----------------
        self.cum_pnl=[]
        self.total_equity=[]
        self.pct_equity=[]

        self.final_order_boook={}




    def _get_market_quantity(self):
        x= self.initial_capital*self.risk
        y=x/self.pip_stop
        z=y/0.0001
        return z

    #The update_signal method simply calls the above method and adds the generated order to the events queue
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)

            self.events.put(order_event)


    def generate_naive_order(self, signal):
        """
        Simply transacts an OrderEvent object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        Parameters:
        signal - The SignalEvent signal information.
        """

        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        # strength = signal.strength
        if len(self.total_equity)>0:
            x=self.total_equity[-1]*self.risk
            y=x/self.pip_stop
            self.mkt_quantity=y/0.0001

        cur_quantity = self.bookEntry.mkt_quantity#self.current_positions[symbol]
        order_type = 'MKT'
        #print (direction,cur_quantity)
        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, self.mkt_quantity, 'BUY')
            self.side = direction
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, self.mkt_quantity, 'SELL')
            self.side = direction

        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
            self.side = direction
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type,abs(cur_quantity), 'BUY')
            self.side = direction
        #order.print_order()

        return order


     #OVERRIDEN METHOD
    def update_fill(self, event):
        """THIS IS A FILL EVENT
            it just executes update_positions_from_fill()
            and update_holdings_from_fill()
            Handles the FillEvent Send by Broker
        """
        if event.type == 'FILL':
            self.update_order_book_from_fill(event)
            self.update_trade_stats_from_fill(event)



    def update_order_book_from_fill(self, fill):
        """
        Takes a FilltEvent object and updates the position matrix
        to reflect the new position.

        Parameters:
        fill - The FillEvent object to update the positions with.
        """
        #Check whether the fill is a buy or a sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        elif fill.direction == 'SELL':
            fill_dir = -1
        b = self.bars.get_latest_bars(fill.symbol)
        #a=orderBookEntry(b[0][1], 100,b[0][5],fill_dir,'MARKET',self.side)
        # print '---------------->',self.side,fill_dir
        self.bookEntry.date = b[0][1]
        self.bookEntry.direction = fill_dir
        self.bookEntry.mkt_quantity += self.mkt_quantity * fill_dir
        self.bookEntry.orderType = 'MARKET'
        self.bookEntry.price = b[0][5]
        self.bookEntry.side = self.side
        #print self.side,'<<<<<<<<<<<<', self.bookEntry




        # self.order_book[self.count] = {'Date':self.bookEntry.date,'quantity': fill_dir*self.mkt_quantity, 'price': self.bookEntry.price,
        #                                         'direction': self.bookEntry.direction,
        #                                         'orderType': self.bookEntry.orderType, 'side': self.bookEntry.side}
        temp=copy.deepcopy(self.bookEntry)
        #self.order_book[self.count]=self.bookEntry
        self.order_book[self.counter]=temp

        self.counter+=1
        self.trade_stats.append(self.order_book.popitem()[1])



    def update_trade_stats_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the holdings matrix
        to reflect the holdings value.

        Parameters:
        fill - The FillEvent object to update the holdings with.
        """

        #check whether the fill is a buy or a sell

        #print '\n'
        # if self.bookEntry.side == 'SHORT' or self.bookEntry.side == 'LONG':
        #     #self.trades.enter_position=self.bookEntry
        #     self.trades.enter_position = self.order_book.popitem(last=True)
        # if self.bookEntry.side == 'EXIT':
        #     #self.trades.exit_position=self.bookEntry
        #     self.trades.exit_position = self.order_book.popitem(last=True)
        # print self.trades

        #print self.order_book

        fill_dir = 0

        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1


        temp_exit=None
        if self.trade_stats[-1].side == 'SHORT' or self.trade_stats[-1].side=='LONG':
            #print self.trade_stats[-1],'<===='
            self.temp_enter_point=self.trade_stats[-1]
        if self.trade_stats[-1].side=='EXIT':
            temp_exit=self.trade_stats[-1]
            #print self.trade_stats[-1],'<====='
        #print '$$$$$$$$$$$$',self.temp_enter_point,temp_exit
        #print self.trade_stats[-1]
        #time.sleep(.5)
        if self.temp_enter_point is not None and temp_exit is not None:
            #print "HERE
            #print self.temp_enter_point,temp_exit,'<-----'
            temp_exit.mkt_quantity= -self.temp_enter_point.mkt_quantity
            self.trades.enter_position=self.temp_enter_point
            self.trades.exit_position= temp_exit

            self.cum_pnl.append(self.trades.pnl())
            setattr(self.trades,'cumulative_pnl',sum(self.cum_pnl))
            if len(self.total_equity)==0:
                self.total_equity.append(self.initial_capital+self.trades.money_earned())
            else:
                self.total_equity.append(self.total_equity[-1]+self.trades.money_earned())

            setattr(self.trades,'total_equity',self.total_equity[-1])

            ab=[ (b - a) / a for a, b in zip(self.total_equity[::1], self.total_equity[1::1])]

            if len(ab)==0:
                self.pct_equity.append((self.total_equity[-1]-self.initial_capital)/self.initial_capital)
            else:
                self.pct_equity.append(ab[-1])

            setattr(self.trades,'pct_equity_change',self.pct_equity[-1])
            self.final_order_boook.setdefault('Date',[]).append(self.trades.enter_position.date)
            self.final_order_boook.setdefault('Quantity',[]).append(self.trades.enter_position.mkt_quantity)
            self.final_order_boook.setdefault('Enter Price',[]).append(self.trades.enter_position.price)
            self.final_order_boook.setdefault('Value',[]).append(abs(self.trades.enter_position.mkt_quantity)*self.trades.enter_position.price)
            self.final_order_boook.setdefault('End',[]).append(self.trades.exit_position.date)
            self.final_order_boook.setdefault('Exit Price',[]).append(self.trades.exit_position.price)
            self.final_order_boook.setdefault('PnL',[]).append(self.trades.pnl())
            self.final_order_boook.setdefault('Money Earned',[]).append(self.trades.money_earned())
            self.final_order_boook.setdefault('Total_equity',[]).append(self.trades.total_equity)
           # print self.trades,'\n'
#

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        #self.equity_curve = curve