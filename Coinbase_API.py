#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 27 22:08:10 2021

@author: robertspringett
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 21:25:08 2021
slght change to order book query
removed hardcoding of secret and key
error record added... need to test

@author: robertspringett
"""


import requests
import pandas as pd
import scipy.stats as stats
import time
from datetime import datetime, timedelta

class CoinBaseAPI():

  def __init__(self):
    
    self.api_url = "https://api.pro.coinbase.com/products/BTC-USD/book?level=3"
    self.session = requests.session()
    self.response = None
    self._json_options = {}

  def query_orderbook(self,Ticker='BTC-USD'):
    api_url = "https://api.pro.coinbase.com/products/"+Ticker+"/book?level=3"
    self.response = self.session.get(api_url)
    if self.response.status_code not in (200, 201, 202):
      self.response.raise_for_status()
    a = self.response.json(**self._json_options)
    bids = pd.DataFrame(a['bids'])
    asks = pd.DataFrame(a['asks'])
    bids.columns = ['Price','Vol',"Unix"]
    asks.columns = ['Price','Vol',"Unix"]
    bids['Vol'] = bids['Vol'].astype(float)
    bids['Price'] = bids['Price'].astype(float)
    asks['Vol'] = asks['Vol'].astype(float)
    asks['Price'] = asks['Price'].astype(float)
    return bids, asks

  def query_ohlcv(self,Ticker='BTC-USD',granularity='3600',start=None,end=None):
    if start ==None:
      api_url = "https://api.pro.coinbase.com/products/"+Ticker+"/candles?granularity="+granularity
    else:
      api_url = "https://api.pro.coinbase.com/products/"+Ticker+"/candles?granularity="+granularity+"&start="+start+'&end='+end
    self.response = self.session.get(api_url)
    if self.response.status_code not in (200, 201, 202):
      self.response.raise_for_status()
    a = self.response.json(**self._json_options)
    ohlc = pd.DataFrame(a)
    ohlc.columns = ['unix','low',"high",'open','close','volume']

    return ohlc

  def download_ohlc_date(self,Ticker='BTC-USD',granularity='3600'):
      start_unix = 1577836800
      end_unix = 1578556800
      start = datetime.utcfromtimestamp(start_unix).isoformat()
      end = datetime.utcfromtimestamp(end_unix).isoformat()
      ohlc = self.query_ohlcv(start=start,end=end)
      i=1
      while end_unix < 1643155200:
        start_unix = end_unix + 3600
        end_unix = start_unix + 720000
        start = datetime.utcfromtimestamp(start_unix).isoformat()
        end = datetime.utcfromtimestamp(end_unix).isoformat()
        temp = self.query_ohlcv(start=start,end=end)
        ohlc = ohlc.append(temp)
        time.sleep(3)
        ohlc.to_csv('/users/robertspringett/Desktop/XBTUSD.csv',index=False)
        print(str(i))
        i = i+1
      ohlc = ohlc.sort_values(by='unix')
      ohlc.to_csv('/users/robertspringett/Desktop/XBTUSD.csv',index=False)
      return ohlc
    