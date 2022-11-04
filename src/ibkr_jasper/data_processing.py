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
    start = trades.select('Date/Time').sort(by=['Date/Time'])[0].to_dicts()[0]['Date/Time']
    return start


def get_port_for_date(portfolio, date, buys, sells):
    buys_asof = buys.filter(pl.col('Date/Time') < date)
    sells_asof = sells.filter(pl.col('Date/Time') < date)
    port_asof = {n: [0] for n in portfolio}
    for etf in portfolio:
        long = buys_asof.filter(pl.col('Symbol') == etf).select('Quantity').sum().fill_null(0).to_numpy()[0, 0]
        short = sells_asof.filter(pl.col('Symbol') == etf).select('Quantity').sum().fill_null(0).to_numpy()[0, 0]
        port_asof[etf] = long + short

    return port_asof
