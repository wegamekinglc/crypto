"""
Created on 2018-6-29

@author: cheng.li
"""

from logbook import Logger
import time
import math
import pandas as pd
from matplotlib import pyplot as plt
from PyFin.Math.Accumulators import MovingAverage as ma
from PyFin.Math.Accumulators import MovingDrawdown as mdd
from PyFin.Math.Accumulators import Max, Sum
from catalyst import run_algorithm
from catalyst.api import symbol, record, order_target_percent
from catalyst.exchange.utils.stats_utils import extract_transactions

plt.style.use('fivethirtyeight')

NAMESPACE = 'tupo_strategy'
log = Logger(NAMESPACE)


def initialize(context):
    context.market = symbol('btc_usd')
    context.long_ma = ma(120, 'close')
    context.mid_ma = ma(60, 'close')
    context.short_ma = ma(20, 'close')
    context.start_time = time.time()

    context.set_commission(maker=0., taker=0.)
    context.set_slippage(spread=0.)
    context.bar_count = 0
    context.twisted_threshold = 0.0008
    context.latest_twisted_ratio = None
    context.direction_threshold = 0.0006
    context.is_twisted = False
    context.is_traded = False
    context.max_twisted_window = 30
    context.break_threshold = 0.008
    context.win_pct = 0.020
    context.loss_pct = 0.006
    context.previous_close = None
    context.trading_data = pd.read_csv('btc_usd.csv', index_col='period_close', parse_dates=True)


def log_twisted_point(context, current_dt, direction, base_price, ratio):
    if context.latest_twisted_ratio is None or ratio < context.latest_twisted_ratio:
        context.latest_twisted_ratio = ratio
        context.direction = direction
        context.is_twisted = True
        context.twisted_point = base_price
        context.twisted_base_count = 0
        log.info('{0}: twisted at {1}'.format(current_dt, base_price))


def clear_twisted_status(context):
    context.is_twisted = False
    context.latest_twisted_ratio = None
    context.twisted_base_count = 0


def clear_positions(context):
    order_target_percent(context.market, 0)
    context.is_traded = False
    clear_twisted_status(context)


def handle_data(context, data):
    current_dt = data.current_dt
    # current = context.trading_data.loc[current_dt + pd.Timedelta(minutes=1)]
    current = data.current(context.market, fields=['close', 'volume'])

    if current['close'] == 0:
        return

    price = dict(close=current['close'])

    if context.previous_close:
        bar_return = math.log(current['close'] / context.previous_close)
    context.previous_close = current['close']

    context.long_ma.push(price)
    context.mid_ma.push(price)
    context.short_ma.push(price)

    record(
        volume=current['volume'],
        price=current['close'],
        sma_s=context.short_ma.result(),
        sma_m=context.mid_ma.result(),
        sma_l=context.long_ma.result(),
    )

    context.bar_count += 1

    if context.bar_count >= 120:
        long_ma = context.long_ma.result()
        mid_ma = context.mid_ma.result()
        short_ma = context.short_ma.result()

        # get the twisted point
        base_price = (long_ma + mid_ma + short_ma) / 3.
        if max(abs(short_ma - long_ma), abs(mid_ma - long_ma), abs(short_ma - mid_ma)) \
                / base_price <= context.twisted_threshold and not context.is_traded:
            ratio = max(abs(short_ma - long_ma), abs(mid_ma - long_ma), abs(short_ma - mid_ma)) / base_price
            # define direction
            if current['close'] > (1. + context.direction_threshold) * base_price:
                log_twisted_point(context, current_dt, 1, base_price, ratio)
            elif current['close'] < (1. - context.direction_threshold) * base_price:
                log_twisted_point(context, current_dt, -1, base_price, ratio)

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
        elif context.is_twisted and context.twisted_base_count > context.max_twisted_window:
            clear_twisted_status(context)

        # win/loss decision
        pos_amount = context.portfolio.positions[context.market].amount
        if pos_amount > 0:
            context.mdd.push({'return': bar_return})
            context.m_high.push({'return': bar_return})
            if current['close'] >= context.cost_price * (1. + context.win_pct): # -context.mdd.result() >= context.win_pct * context.m_high.result() and context.m_high.result() > 0.:
                clear_positions(context)
            elif current['close'] <= context.cost_price * (1. - context.loss_pct):
                clear_positions(context)
        elif pos_amount < 0:
            context.mdd.push({'return': -bar_return})
            context.m_high.push({'return': -bar_return})
            if current['close'] <= context.cost_price * (1. - context.win_pct): # -context.mdd.result() >= context.win_pct * context.m_high.result() and context.m_high.result() > 0.:
                clear_positions(context)
            elif current['close'] >= context.cost_price * (1. + context.loss_pct):
                clear_positions(context)


def analyze(context=None, perf=None):
    quote_currency = list(context.exchanges.values())[0].quote_currency.upper()

    # Plot the portfolio value over time.
    ax1 = plt.subplot(211)
    perf.loc[:, 'portfolio_value'].plot(ax=ax1, label='pnl')
    ax1.set_ylabel('Portfolio\nValue\n({})'.format(quote_currency))

    # Plot the price increase or decrease over time.
    ax2 = plt.subplot(212, sharex=ax1)
    perf.loc[:, 'price'].plot(ax=ax2, label='Price', linewidth=0.5)
    perf.loc[:, 'sma_s'].plot(ax=ax2, label='sma_s', c='yellow', linewidth=0.25)
    perf.loc[:, 'sma_m'].plot(ax=ax2, label='sma_m', c='orange', linewidth=0.25)
    perf.loc[:, 'sma_l'].plot(ax=ax2, label='sma_l', c='purple', linewidth=0.25)

    ax2.set_ylabel('{asset}\n({quote})'.format(
        asset=context.market.symbol, quote=quote_currency
    ))

    transaction_df = extract_transactions(perf)
    if not transaction_df.empty:
        buy_df = transaction_df[transaction_df['amount'] > 0]
        sell_df = transaction_df[transaction_df['amount'] < 0]
        ax2.scatter(
            buy_df.index.to_pydatetime(),
            perf.loc[buy_df.index.floor('1 min'), 'price'],
            marker='^',
            s=50,
            c='green',
            label=''
        )
        ax2.scatter(
            sell_df.index.to_pydatetime(),
            perf.loc[sell_df.index.floor('1 min'), 'price'],
            marker='v',
            s=50,
            c='red',
            label=''
        )


if __name__ == '__main__':
    import os
    import tempfile
    import pandas as pd
    from catalyst.utils.paths import ensure_directory

    folder = os.path.join(
        tempfile.gettempdir(), 'catalyst', NAMESPACE
    )
    ensure_directory(folder)

    time_str = time.strftime('%Y%m%d-%H%M%S')
    out = os.path.join("d:/", '{}.p'.format(time_str))
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
                       output=out,
                       fast_backtest=True
    )
    log.info('saved perf stats: {}'.format(out))
    plt.show()
