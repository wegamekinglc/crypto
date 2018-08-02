"""
Created on 2018-7-10

@author: cheng.li
"""

import io
import pandas as pd
import sqlalchemy as sa

engine = sa.create_engine('postgresql+psycopg2://postgres:A12345678!@10.63.6.220/crypto')
conn = engine.raw_connection()
cur = conn.cursor()

tables = ['bitfinex', 'binance']

table = tables[0]

query = f"select distinct pair from {table}_bars_1min"
df = pd.read_sql(query, con=engine)

for pair in df.pair:
    res = pd.read_sql(f"select * from {table}_bars_1min where pair = '{pair}'",
                      con=engine,
                      parse_dates=['trade_time'])
    res['trade_time'] = res['trade_time'].values + pd.Timedelta(minutes=1)
    query = f"delete from {table}_bars_1min where pair = '{pair}'"
    engine.execute(query)
    output = io.StringIO()
    res.to_csv(output, sep='\t', header=False, index=False, na_rep='null')
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, f'{table}_bars_1min', null='null')
    conn.commit()
    print(f"{pair} is finished")

print(df)