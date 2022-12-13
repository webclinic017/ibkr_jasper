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


def adjust_trades_by_splits(trades, tickers, splits):
    trades_total_adj = []
    for ticker in tickers:
        cur_trades_adj = (trades
                          .filter(pl.col('ticker') == ticker)
                          .join_asof(splits
                                     .filter(pl.col('ticker') == ticker)
                                     .drop('ticker'),
                                     on='datetime', strategy='forward')
                          .with_columns([(pl.col('quantity') * pl.col('coef')).alias('quantity'),
                                         (pl.col('price') / pl.col('coef')).alias('price')])
                          .drop('coef'))
        trades_total_adj.append(cur_trades_adj)

    trades_adj = pl.concat(trades_total_adj)
    return trades_adj
