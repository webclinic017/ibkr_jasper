import polars as pl

def get_etf_buys(trades):
    etf_buys = (trades.filter((pl.col('DataDiscriminator') == 'Trade') &
                              (pl.col('Asset Category') == 'Stocks') &
                              (pl.col('Quantity') > 0))
                      .drop(['DataDiscriminator', 'Asset Category']))
    return etf_buys


def get_etf_sells(trades):
    etf_sells = (trades.filter((pl.col('DataDiscriminator') == 'Trade') &
                               (pl.col('Asset Category') == 'Stocks') &
                               (pl.col('Quantity') > 0))
                       .drop(['DataDiscriminator', 'Asset Category']))
    return etf_sells