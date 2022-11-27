from datetime import timedelta

import numpy as np
import polars as pl


def get_etf_buys(trades):
    etf_buys = (trades
                .filter((pl.col('asset_type') == 'Stocks') &
                        (pl.col('quantity') > 0)))
    return etf_buys


def get_etf_sells(trades):
    etf_sells = (trades
                 .filter((pl.col('asset_type') == 'Stocks') &
                         (pl.col('quantity') < 0)))
    return etf_sells


def get_all_etfs(trades):
    all_tickers = np.transpose(trades.select('ticker').unique().to_numpy()).tolist()[0]
    etfs = [x for x in all_tickers if len(x) <= 4]
    return etfs


def get_portfolio_start_date(trades):
    start = min(trades['datetime']).date()
    return start
