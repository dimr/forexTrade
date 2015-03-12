import datetime
import os, os.path
import pandas as pd
from abc import ABCMeta, abstractmethod
import os
from event import MarketEvent
import sys
import numpy as np

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """
    __metaclass__ = ABCMeta


    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("should be implemented, get_latest_bars")


    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("should be implemented, update_bars")



class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self,events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        #print "Running:",type(self).__name__,"on ",symbol_list
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}



        self.latest_symbol_data = {}
        self.continue_backtest = True


        self.symbols_counter={}
        for s in symbol_list:
            #c=self.custom_gen()
            self.symbols_counter[s]=self.custom_gen()
        self._open_csv()

        #some_data={}


    def _open_csv(self):
        comb_index = None
        # for s in self.symbol_list:
        #     print 'opening',os.path.join(self.csv_dir,os.listdir(self.csv_dir))
        #     self.symbol_data[s] = pd.read_csv(os.path.join(self.csv_dir,os.listdir(self.csv_dir)),index_col=0,names=['date','open','high','low','close','volume'],header=0)

        for s in self.symbol_list:
            # self.symbol_data[s]=pd.read_csv(os.path.join(self.csv_dir,'%s.csv'%s),usecols=[1,2,3,4,5,6] )
            #
            # self.symbol_data[s]['DATE']=self.symbol_data[s]["<DATE>"].map(str) +" " +self.symbol_data[s]["<TIME>"]
            # self.symbol_data[s].index = pd.to_datetime(self.symbol_data[s].pop('DATE'))
            # del self.symbol_data[s]['<TIME>']
            # del self.symbol_data[s]['<DATE>']
            # self.symbol_data[s].columns= ['open','low','high','close']
            # #self.symbol_data[s] =some_data


            self.symbol_data[s] = pd.read_csv(os.path.join(self.csv_dir,'%s.csv'%s),index_col=[0])
            self.symbol_data[s].columns=['open','low','high','close']
            self.symbol_data[s].index.name='Datetime'
            self.symbol_data[s] = np.around(self.symbol_data[s],decimals=4)
            self.symbol_data[s].index = pd.to_datetime(self.symbol_data[s].index.values)

            # self.symbol_data[s] = pd.read_csv(os.path.join(self.csv_dir,'%s.csv'%s),parse_dates={'Date': ['<DATE>','<TIME>']},index_col='Date',usecols=['<DATE>','<TIME>', '<OPEN>','<LOW>','<HIGH>','<CLOSE>'])
            # self.symbol_data[s].colums=['open','low','high','close']





            #self.symbol_data[s] = pd.read_csv(os.path.join(self.csv_dir,'%s.csv'%s),index_col=0,names=['date','open','high','low','close','volume'],header=0)
#            self.symbol_data[s] = np.around(self.symbol_data[s],decimals=4)
            #print self.symbol_data[s]
            #print self.symbol_data[s]


            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)
            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()


        #for f in os.listdir(self.csv_dir):
            #print 'Opening: ',f
         #   self.symbol_data[self.symbol_list[0]] = pd.read_csv(os.path.join(self.csv_dir,f),index_col=0,names=['date','open','high','low','close','volume'],header=0)
        #### to values() giati einai list?
        #
        # if comb_index is None:
        #     comb_index = self.symbol_data
       # print comb_index
       # print self.symbol_data[self.symbol_list[0]]
       #  print sys._getframe().f_code.co_name
       #  print type(self.symbol_data),self.symbol_data.keys(),type(self.symbol_data[self.symbol_list[0]])
       #  for s in self.symbol_list:
       #      print self.symbol_data[s].columns


    def custom_gen(self,start=0):
        x=start
        while True:
            yield x
            x+=1


    def _get_new_bar(self,symbol):
         """
        Returns the latest bar from the data feed as a tuple of
        (sybmbol, datetime, open, low, high, close).
        """
         #print sys._getframe().f_code.co_name
         #print self.symbol_data[self.symbol_list[0]]
         #print type(self.symbol_data[symbol])

         for b in self.symbol_data[symbol]:
            try:
                c = self.symbols_counter[symbol].next()
            except StopIteration:
                print "STOPPED"
                self.continue_backtest = False
            #else:
            try:
                temp_date = self.symbol_data[symbol].irow(c).name
                temp_open = self.symbol_data[symbol].irow(c)['open']
                temp_high = self.symbol_data[symbol].irow(c)['high']
                temp_low = self.symbol_data[symbol].irow(c)['low']
                temp_close = self.symbol_data[symbol].irow(c)['close']
            except IndexError:
                print "EXIT"
                return
                # temp_open = self.symbol_data[symbol].irow(c)[2]
                # temp_high = self.symbol_data[symbol].irow(c)[3]
                # temp_low = self.symbol_data[symbol].irow(c)[4]
                # temp_close = self.symbol_data[symbol].irow(c)[5]
                #temp_volume = self.symbol_data[symbol].irow(c)['volume']
            yield tuple([symbol,temp_date,temp_open,temp_high,temp_low,temp_close])



              # HistoricCSVDataHandler.counter=HistoricCSVDataHandler.counter+1
              # #print '--------',tuple([symbol,  datetime.datetime.strptime(b[0], "%Y.%m.%d %H:%M"),b[1][0], b[1][1], b[1][2],b[1][3],b[1][4]])
              # #print b,row[0]
              # #yield tuple([symbol, datetime.datetime.strptime(row.name,"%Y.%m.%d %H:%M"),row[0],row[1],row[2],row[3],row[4]])
              # print b
              # yield tuple([symbol,  datetime.datetime.strptime(b[0], "%Y.%m.%d %H:%M"),b[1][0], b[1][1], b[1][2],b[1][3],b[1][4]])


    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        #print sys._getframe().f_code.co_name
        try:

            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set.",self.symbol_list
        else:
            d=bars_list[-N:]
            return d


    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """


        for s in self.symbol_list:
            try:
                #print "in try"
                gen=self._get_new_bar(s)

                bar = next(gen)
                #print bar

            except StopIteration:
                self.continue_backtest = False
                #print "in except"
            else:
                if bar is not None:
                    #print "in else"
                    self.latest_symbol_data[s].append(bar)
                    # print self.latest_symbol_data[s]
        self.events.put(MarketEvent())
        # #print sys._getframe().f_code.co_name


