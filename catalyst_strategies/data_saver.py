"""
Created on 2018-7-1

@author: cheng.li
"""

from logbook import Logger
import pandas as pd

from catalyst.api import record, symbol
from catalyst.utils.run_algo import run_algorithm


NAMESPACE = 'data_saver'
log = Logger(NAMESPACE)


def initialize(context):
   context.asset = symbol('btc_usd')


def handle_data(context, data):
   fields = data.current(context.asset, ['price', 'open', 'high', 'low', 'close', 'volume'])
   record(price=fields['price'],
          open=fields['open'],
          high=fields['high'],
          low=fields['low'],
          close=fields['close'],
          volume=fields['volume'])
   log.info('{0}'.format(data.current_dt))


def analyze(context=None, results=None):
   data = results[['price', 'open', 'high', 'low', 'close', 'volume']]
   data.to_csv('btc_usd.csv')


start = pd.to_datetime('2017-07-01', utc=True)
end = pd.to_datetime('2018-06-08', utc=True)
results = run_algorithm(initialize=initialize,
                        handle_data=handle_data,
                        analyze=analyze,
                        start=start,
                        end=end,
                        data_frequency='minute',
                        exchange_name='bitfinex',
                        capital_base=10000,
                        quote_currency='usd')