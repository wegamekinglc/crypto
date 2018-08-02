"""
Created on 2018-7-2

@author: cheng.li
"""

import io
import pandas as pd
import numpy as np
import sqlalchemy as sa
from catalyst.exchange.utils.factory import get_exchange
import catalyst.exchange.exchange_bcolz as bz


engine = sa.create_engine('postgresql+psycopg2://postgres:A12345678!@10.63.6.220/crypto')
exchange_name = 'bitfinex'
exchange = get_exchange(exchange_name=exchange_name,
                        quote_currency='usd',
                        must_authenticate=False,
                        skip_init=True,
                        auth_alias=None)

reader = bz.BcolzExchangeBarReader(rootdir=r'C:\Users\wegamekinglc\.catalyst\data\exchanges\{0}\minute_bundle'.format(exchange_name),
                                   data_frequency='minute')

exchange.init()
assets = exchange.assets

sids = [a.sid for a in assets]

start_dt = pd.to_datetime('2017-07-01')
end_dt = pd.to_datetime('2018-07-08 23:59:00')

periods = pd.date_range(start_dt, end_dt, freq='T') + pd.Timedelta(minutes=1)

conn = engine.raw_connection()
cur = conn.cursor()

for sid, asset in zip(sids, assets):
    try:
        pair = asset.asset_name.replace(' / ', '|').strip()
        data = reader.load_raw_arrays(['open', 'high', 'low', 'close', 'volume'], start_dt, end_dt, [sid])
        df = pd.DataFrame(
            {'open': data[0].flatten(),
             'high': data[1].flatten(),
             'low': data[2].flatten(),
             'close': data[3].flatten(),
             'volume': data[4].flatten()},
            index=periods
        )
        df['pair'] = pair
        df.index.name = 'trade_time'
        df.reset_index(inplace=True)
        df = df[['trade_time', 'pair', 'open', 'high', 'low', 'close', 'volume']]
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='null')
        output.seek(0)
        contents = output.getvalue()
        cur.copy_from(output, '{0}_bars_1min'.format(exchange_name), null='null')  # null values become ''
        conn.commit()
        print("{0} is finished".format(asset.asset_name))
    except Exception as e:
        print(e)
        print(asset)

