from datetime import timedelta

import numpy as np
import polars as pl


def get_etf_buys(trades):
    etf_buys = (trades
                .filter((pl.col('DataDiscriminator') == 'Trade') &
                        (pl.col('Asset Category') == 'Stocks') &
                        (pl.col('Quantity') > 0))
                .drop(['DataDiscriminator', 'Asset Category']))
    return etf_buys


def get_etf_sells(trades):
    etf_sells = (trades
                 .filter((pl.col('DataDiscriminator') == 'Trade') &
                         (pl.col('Asset Category') == 'Stocks') &
                         (pl.col('Quantity') < 0))
                 .drop(['DataDiscriminator', 'Asset Category']))
    return etf_sells


def get_all_etfs(trades):
    all_tickers = np.transpose(trades.select('Symbol').unique().to_numpy()).tolist()[0]
    etfs = [x for x in all_tickers if len(x) <= 4]
    return etfs


def get_portfolio_start_date(trades):
    start = (trades
             .select(pl.col('Date/Time').min())
             .with_column(pl.col('Date/Time').cast(pl.Date))
             .to_struct('')[0]['Date/Time'])
    return start


def get_port_for_date(portfolio, date, buys, sells):
    """Gives portfolio value on previous day close"""
    buys_asof = buys.filter(pl.col('Date/Time') < date)
    sells_asof = sells.filter(pl.col('Date/Time') < date)
    port_asof = {n: [0] for n in portfolio}
    for etf in portfolio:
        long = buys_asof.filter(pl.col('Symbol') == etf).select('Quantity').sum().fill_null(0).to_numpy()[0, 0]
        short = sells_asof.filter(pl.col('Symbol') == etf).select('Quantity').sum().fill_null(0).to_numpy()[0, 0]
        port_asof[etf] = long + short

    return port_asof


def get_portfolio_value(port, prices, date):
    """Gives portfolio value on previous day close prices"""
    total_value = 0
    for etf, pos in port.items():
        if pos == 0:
            continue

        price = (prices
                 .filter(pl.col('Date') < date)
                 .select(pl.col(etf))
                 .reverse()
                 .limit(1)
                 .to_numpy()[0, 0])
        total_value += pos * price

    return total_value


def get_cur_month_deals_value(start_date, buys, sells):
    end_date = (start_date + timedelta(days=32)).replace(day=1)
    deals_cur_month = (buys
                       .select(['Date/Time', 'Quantity', 'T. Price', 'Comm/Fee'])
                       .extend(sells
                               .select(['Date/Time', 'Quantity', 'T. Price', 'Comm/Fee']))
                       .filter((start_date <= pl.col('Date/Time')) & (pl.col('Date/Time') < end_date))
                       .with_column((pl.col('Quantity') * pl.col('T. Price') - pl.col('Comm/Fee')).alias('value'))
                       .select(pl.col('value'))
                       .sum()
                       .fill_null(0)
                       .to_numpy()[0, 0])

    return deals_cur_month


def get_cur_month_divs(start_date, divs):
    end_date = (start_date + timedelta(days=32)).replace(day=1)
    divs_cur_month = (divs
                      .filter((start_date <= pl.col('Date')) & (pl.col('Date') < end_date))
                      .select(pl.col('Amount'))
                      .sum()
                      .fill_null(0)
                      .to_numpy()[0, 0])

    return divs_cur_month


def get_period_return(start_date, end_date, etfs, buys, sells, divs, prices):
    trade_dates = (buys
                   .select('Date/Time')
                   .extend(sells
                           .select('Date/Time'))
                   .with_column(pl.col('Date/Time').cast(pl.Date))
                   .rename({'Date/Time': 'Date'})
                   .extend(divs
                           .select('Date'))
                   .filter((start_date <= pl.col('Date')) & (pl.col('Date') < end_date))
                   .unique()
                   .get_column('Date')
                   .to_numpy()
                   .tolist())
    trade_dates = sorted(trade_dates + [start_date.date(), end_date.date()])

    return_total = 1
    value_prev = None
    for date in trade_dates:
        # TODO put all three in one array
        buys_today = (buys
                      .filter(pl.col('Date/Time').cast(pl.Date) == date)
                      .with_column((pl.col('Quantity') * pl.col('T. Price') - pl.col('Comm/Fee')).alias('sum'))
                      .select('sum')
                      .sum()
                      .fill_null(0)
                      .to_numpy()[0, 0])
        sells_today = (sells
                       .filter(pl.col('Date/Time').cast(pl.Date) == date)
                       .with_column((pl.col('Quantity') * pl.col('T. Price') - pl.col('Comm/Fee')).alias('sum'))
                       .select('sum')
                       .sum()
                       .fill_null(0)
                       .to_numpy()[0, 0])
        divs_today = (divs
                      .filter(pl.col('Date').cast(pl.Date) == date)
                      .select('Amount')
                      .sum()
                      .fill_null(0)
                      .to_numpy()[0, 0])
        delta_today = buys_today + sells_today + divs_today

        port_morning = get_port_for_date(etfs, date, buys, sells)
        value_morning = get_portfolio_value(port_morning, prices, date)
        port_evening = get_port_for_date(etfs, date + timedelta(days=1), buys, sells)
        value_evening = get_portfolio_value(port_evening, prices, date + timedelta(days=1))

        if value_prev is None:
            value_prev = value_morning

        if delta_today == 0:
            continue

        return_prev = value_morning / value_prev if value_prev > 0 else 1
        return_today = (value_evening - delta_today) / value_morning
        return_total *= return_prev * return_today

        value_prev = value_evening

    return return_total - 1
