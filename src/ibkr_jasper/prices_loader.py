import polars as pl
import yfinance as yf
from datetime import date


def load_etf_prices(all_etfs, start_date):
    prices_dict = {}
    for ticker in all_etfs:
        prices = yf.download(ticker,
                             start=start_date.date(),
                             end=date.today(),
                             progress=False)
        prices_dict[ticker] = pl.DataFrame(prices['Close'].reset_index()).rename({'Close': ticker})

    etf_prices = None
    for ticker, prices in prices_dict.items():
        if etf_prices is None:
            etf_prices = prices
        else:
            etf_prices = etf_prices.join(prices, on='Date', how='outer')

    return etf_prices
