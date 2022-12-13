import pickle
import polars as pl
import yfinance as yf
from pathlib import Path
from datetime import date, timedelta
from pandas._libs.tslibs.offsets import BDay

from src.ibkr_jasper.timer import Timer

PRICES_PICKLE_PATH = Path('../../data') / 'prices.pickle'
SPLITS_PICKLE_PATH = Path('../../data') / 'splits.pickle'


def load_prices_and_splits(all_tickers, start_date):
    first_business_day = (start_date - BDay(1)).to_pydatetime().date()
    last_business_day = (date.today() - BDay(1)).to_pydatetime().date()

    # try to load cache data and if something is missing, then reload all prices
    try:
        with open(PRICES_PICKLE_PATH, 'rb') as handle:
            prices = pickle.load(handle)
        with open(SPLITS_PICKLE_PATH, 'rb') as handle:
            splits = pickle.load(handle)

        saved_min_date = (prices
                          .select(pl.col('date').min())
                          .to_struct('')[0]['date'])
        saved_max_date = (prices
                          .select(pl.col('date').max())
                          .to_struct('')[0]['date'])
        saved_etfs = prices.columns[1:]

        if (set(saved_etfs) == set(all_tickers) and
                saved_min_date == first_business_day and
                saved_max_date == last_business_day):
            return prices, splits
        else:
            print('Cache file with prices misses some values')
    except FileNotFoundError:
        print('Cache file with prices does not exist')

    with Timer('Full reload of prices from yahoo', True):
        data = yf.download(all_tickers, start=first_business_day, end=last_business_day + BDay(1), actions=True)
        prices = (pl.from_pandas(data['Close'].reset_index())
                    .rename({'Date': 'date'})
                    .with_column(pl.col('date').cast(pl.Date)))
        today_splits = (pl.DataFrame({'datetime': [date.today() + timedelta(days=1)] * len(all_tickers),
                                      'ticker': all_tickers,
                                      'splits': [1.0] * len(all_tickers)})
                        .with_column(pl.col('datetime').cast(pl.Datetime)))
        splits = (pl.from_pandas(data['Stock Splits'].reset_index())
                    .rename({'Date': 'datetime'})
                    .melt(id_vars='datetime', variable_name='ticker', value_name='splits')
                    .filter(pl.col('splits') > 0)
                    .with_column(pl.col('datetime').cast(pl.Datetime))
                    .vstack(today_splits))
        splits_list = []

        # do not know how to optimize it
        # TODO use partition_by
        for ticker in all_tickers:
            cur_splits = (splits
                          .filter(pl.col('ticker') == ticker)
                          .with_columns([pl.col('ticker').cast(pl.Categorical),
                                         pl.col('splits').cumprod(reverse=True).alias('coef')])
                          .drop('splits'))
            splits_list.append(cur_splits)

        splits = pl.concat(splits_list)

    with open(PRICES_PICKLE_PATH, 'wb') as handle:
        pickle.dump(prices, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(SPLITS_PICKLE_PATH, 'wb') as handle:
        pickle.dump(splits, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return prices, splits
