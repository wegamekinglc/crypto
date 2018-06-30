"""
Created on 2018-6-29

@author: cheng.li
"""

from logbook import Logger
import time
import math
from PyFin.Math.Accumulators import MovingAverage as ma
from PyFin.Math.Accumulators import MovingDrawdown as mdd
from PyFin.Math.Accumulators import Max, Sum
from catalyst import run_algorithm
from catalyst.api import symbol, order_target_percent

NAMESPACE = 'tupo_strategy'
log = Logger(NAMESPACE)


def initialize(context):
    context.market = symbol('btc_usd')
    context.long_ma = ma(120, 'close')
    context.mid_ma = ma(30, 'close')
    context.short_ma = ma(10, 'close')
    context.start_time = time.time()

    context.set_commission(maker=0., taker=0.)
    context.set_slippage(spread=0.)
    context.bar_count = 0
    context.twisted_threshold = 0.001
    context.direction_threshold = 0.001
    context.is_twisted = False
    context.is_traded = False
    context.max_twisted_window = 10
    context.break_threshold = 0.015
    context.win_pct = 0.30
    context.loss_pct = 0.006
    context.previous_close = None


def log_twisted_point(context, data, direction, base_price):
    context.direction = direction
    context.is_twisted = True
    context.twisted_point = base_price
    context.twisted_base_count = 0
    log.info('{0}: twisted at {1}'.format(data.current_dt, base_price))


def handle_data(context, data):
    current = data.current(context.market, fields=['close'])

    if current['close'] == 0:
        return

    price = dict(close=current['close'])

    if context.previous_close:
        bar_return = math.log(current['close'] / context.previous_close)
    context.previous_close = current['close']

    context.long_ma.push(price)
    context.mid_ma.push(price)
    context.short_ma.push(price)

    context.bar_count += 1

    if context.bar_count >= 120:
        long_ma = context.long_ma.result()
        mid_ma = context.mid_ma.result()
        short_ma = context.short_ma.result()

        # get the twisted point
        base_price = (long_ma + mid_ma + short_ma) / 3.
        if max(abs(short_ma - long_ma), abs(mid_ma - long_ma), abs(short_ma - mid_ma)) \
                / base_price <= context.twisted_threshold and not context.is_twisted:
            # define direction
            if current['close'] > (1. + context.direction_threshold) * base_price:
                log_twisted_point(context, data, 1, base_price)
            elif current['close'] < (1. - context.direction_threshold) * base_price:
                log_twisted_point(context, data, -1, base_price)

        # define break point
        if context.is_twisted and context.twisted_base_count <= context.max_twisted_window and not context.is_traded:
            context.twisted_base_count += 1
            if context.direction == 1:
                if current['close'] >= (1. + context.break_threshold) * context.twisted_point:
                    order_target_percent(
                        context.market, 1
                    )
                    context.cost_price = current['close']
                    context.is_traded = True
                    context.mdd = mdd(5000, 'return')
                    context.m_high = Max(Sum('return'))
            elif context.direction == -1:
                if current['close'] <= (1. - context.break_threshold) * context.twisted_point:
                    order_target_percent(
                        context.market, -1
                    )
                    context.cost_price = current['close']
                    context.is_traded = True
                    context.mdd = mdd(5000, 'return')
                    context.m_high = Max(Sum('return'))
        else:
            context.is_twisted = False

        # win/loss decision
        pos_amount = context.portfolio.positions[context.market].amount
        if pos_amount > 0:
            context.mdd.push({'return': bar_return})
            context.m_high.push({'return': bar_return})
            if -context.mdd.result() >= context.win_pct * context.m_high.result() and context.m_high.result() > 0.:
                order_target_percent(context.market, 0)
                context.is_twisted = False
                context.is_traded = False
            elif current['close'] <= context.cost_price * (1. - context.loss_pct):
                order_target_percent(context.market, 0)
                context.is_twisted = False
                context.is_traded = False
        elif pos_amount < 0:
            context.mdd.push({'return': -bar_return})
            context.m_high.push({'return': -bar_return})
            if -context.mdd.result() >= context.win_pct * context.m_high.result() and context.m_high.result() > 0.:
                order_target_percent(context.market, 0)
                context.is_twisted = False
                context.is_traded = False
            elif current['close'] >= context.cost_price * (1. + context.loss_pct):
                order_target_percent(context.market, 0)
                context.is_twisted = False
                context.is_traded = False


def analyze(context=None, perf=None):
    pass


if __name__ == '__main__':
    import os
    import tempfile
    import pandas as pd
    from catalyst.utils.paths import ensure_directory

    folder = os.path.join(
        tempfile.gettempdir(), 'catalyst', NAMESPACE
    )
    ensure_directory(folder)

    timestr = time.strftime('%Y%m%d-%H%M%S')
    out = os.path.join(folder, '{}.p'.format(timestr))
    df = run_algorithm(capital_base=10000.,
                       data_frequency='minute',
                       initialize=initialize,
                       handle_data=handle_data,
                       analyze=analyze,
                       exchange_name='bitfinex',
                       algo_namespace=NAMESPACE,
                       quote_currency='usd',
                       start=pd.to_datetime('2018-01-01', utc=True),
                       end=pd.to_datetime('2018-06-08', utc=True),
                       output=out
    )
    log.info('saved perf stats: {}'.format(out))