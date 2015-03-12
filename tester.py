import datetime
import pandas as pd
import os
import time
import numpy as np
csv_dir = '/home/dimitris/workspace/python/PycharmProjects/quant/data'
# symbol_data={}
symbols=['USDEUR','GBPEUR']
# for s in symbols:
#     symbol_data[s]=pd.read_csv(os.path.join(csv_dir,'%s.csv'%s),index_col=0,names=['date','open','high','low','close','volume'],header=0)
#
#
# #print symbol_data[symbols[0]]
#
#
# def get_new_bar(symbol):
#     for b in symbol_data[symbol].iterrows():
#         #print i,b
#         yield tuple([symbol,  datetime.datetime.strptime(b[0], "%Y.%m.%d %H:%M"),b[1][0], b[1][1], b[1][2],b[1][3],b[1][4]])
#
#
# gen1=get_new_bar(symbols[0])
# gen2 = get_new_bar(symbols[1])
# #
# # import time
# #
#
# while True:
#     print gen1.next(),gen2.next()
#     time.sleep(1)

# for i in get_new_bar(symbols[0]):
#     print i


class Handler(object):
    def __init__(self,csv_dir,symbol_list):
        #self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}


        Handler.counter=0
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_csv()
        self.counter=0
        self.num_of_counters = len(symbol_list)

        self.symbols_counter={}

        for s in symbol_list:
            #c=self.custom_gen()
            self.symbols_counter[s]=self.custom_gen()


    def _open_csv(self):
        comb_index = None
        # for s in self.symbol_list:
        #     print 'opening',os.path.join(self.csv_dir,os.listdir(self.csv_dir))
        #     self.symbol_data[s] = pd.read_csv(os.path.join(self.csv_dir,os.listdir(self.csv_dir)),index_col=0,names=['date','open','high','low','close','volume'],header=0)

        for s in self.symbol_list:
            self.symbol_data[s] = pd.read_csv(os.path.join(self.csv_dir,'%s.csv'%s),index_col=0,names=['date','open','high','low','close','volume'],header=0)
            self.symbol_data[s] = np.around(self.symbol_data[s],decimals=4)



    def custom_gen(self,start=0):
        x=start
        while True:
            yield x
            x+=1



    def _get_new_bar(self,symbol):

        for b in self.symbol_data[symbol]:
            #print '--------------',symbol,'-->',self.counter
            #yield tuple([symbol,  datetime.datetime.strptime(b[0], "%Y.%m.%d %H:%M"),b[1][0], b[1][1], b[1][2],b[1][3],b[1][4]])
            c=self.symbols_counter[symbol].next()
            temp_date = self.symbol_data[symbol].irow(c).name
            temp_open = self.symbol_data[symbol].irow(c)['open']
            temp_high = self.symbol_data[symbol].irow(c)['high']
            temp_low = self.symbol_data[symbol].irow(c)['low']
            temp_close = self.symbol_data[symbol].irow(c)['close']
            yield tuple([symbol,temp_date,temp_open,temp_high,temp_low,temp_close])
            #yield([symbol,datetime.datetime.strptime(b[0].name, "%Y.%m.%d %H:%M")])




    def get_latest_bars(self,symbol,N=1):
        #print type(self.latest_symbol_data),self.latest_symbol_data.keys()
        ss=self._get_new_bar(symbol)
        bar=ss.next()

        #print bar
        return bar




h = Handler(csv_dir,symbols)
while True:
    for s in symbols:
        b=h._get_new_bar(s)
        print h.get_latest_bars(s,N=1)

        #print s
        #print b
    #print next(h._get_new_bar('USDEUR'))
    time.sleep(1)
    print '----------------------------'