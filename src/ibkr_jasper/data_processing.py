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


def get_tlh_trades(trades_total, tickers_total, all_prices, xrub_rates):
    """
    Tax Loss Harvesting
    """
    filtered_trades = []
    for ticker in tickers_total:
        cur_price = pl.last(all_prices[ticker])
        cur_trades = trades_total.filter(pl.col('ticker') == ticker)
        cur_buys = (cur_trades
                    .filter(pl.col('quantity') > 0)
                    .with_columns([
                        pl.col('datetime').cast(pl.Date).alias('date'),
                        pl.lit(cur_price).alias('cur_price'),
                        pl.min([0, cur_price - pl.col('price')]).alias('diff'),
                    ])
                    .join(xrub_rates, on=['date', 'curr'])
                    .drop(['datetime', 'asset_type', 'code', 'curr'])
                    .with_columns([
                        (pl.col('price') * pl.col('rate')).alias('price_rub'),
                        (pl.col('cur_price') * pl.col('rate')).alias('cur_price_rub'),
                    ])
                    .with_column((pl.col('quantity') * pl.min([0, pl.col('cur_price_rub') - pl.col('price_rub')])).alias('diff_rub')))
        cur_sells_sum = (cur_trades
                         .filter(pl.col('quantity') < 0)
                         .drop(['asset_type', 'code'])
                         ['quantity']
                         .sum())
        cur_sells_sum = 0 if cur_sells_sum is None else cur_sells_sum
        cur_buys_sum = cur_buys['quantity'].sum()
        if not (cur_sells_sum is None) and (cur_buys_sum + cur_sells_sum == 0):
            continue
        if cur_buys['diff'].sum() == 0:
            continue

        for row in cur_buys.to_dicts():
            if cur_sells_sum == 0:
                pass
            elif cur_sells_sum + row['quantity'] < 0:
                cur_sells_sum += row['quantity']
                row['quantity'] = 0
            else:
                row['quantity'] += cur_sells_sum
                cur_sells_sum = 0

            filtered_trades.append(row)

    tlh_trades = (pl.from_dicts(filtered_trades)
                    .select(cur_buys.columns)
                    .filter((pl.col('quantity') > 0) & (pl.col('diff') < 0))
                    .sort('diff_rub'))

    return tlh_trades
