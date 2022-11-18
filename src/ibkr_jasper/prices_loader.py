import pickle
import polars as pl
import yfinance as yf
from pathlib import Path
from datetime import date
from pandas._libs.tslibs.offsets import BDay

from src.ibkr_jasper.timer import Timer

PRICES_PICKLE_PATH = Path('../../data') / 'prices.pickle'


def load_etf_prices(all_etfs, start_date):
    first_business_day = (start_date - BDay(1)).to_pydatetime().date()
    last_business_day = (date.today() - BDay(1)).to_pydatetime().date()

    # try to load cache data and if something is missing, then reload all prices
    try:
        with open(PRICES_PICKLE_PATH, 'rb') as handle:
            etf_prices = pickle.load(handle)

        saved_min_date = (etf_prices
                          .select(pl.col('Date').min())
                          .to_struct('')[0]['Date'])
        saved_max_date = (etf_prices
                          .select(pl.col('Date').max())
                          .to_struct('')[0]['Date'])
        saved_etfs = etf_prices.columns[1:]

        if (set(saved_etfs) == set(all_etfs) and
                saved_min_date == first_business_day and
                saved_max_date == last_business_day):
            return etf_prices
        else:
            print('Cache file with prices misses some values')
    except FileNotFoundError:
        print('Cache file with prices does not exist')

    with Timer('Full reload of prices from yahoo', True):
        prices = yf.download(all_etfs, start=first_business_day, end=last_business_day + BDay(1))
        etf_prices = (pl.from_pandas(prices['Close'].reset_index())
                      .with_column(pl.col('Date').cast(pl.Date)))

    with open(PRICES_PICKLE_PATH, 'wb') as handle:
        pickle.dump(etf_prices, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return etf_prices
