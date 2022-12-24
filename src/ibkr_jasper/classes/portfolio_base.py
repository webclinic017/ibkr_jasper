import polars as pl
from datetime import date
from pandas._libs.tslibs.offsets import BDay
from pathlib import Path


class PortfolioBase:
    PORTFOLIOS_PATH = Path('../../portfolios')

    def __init__(self):
        self.trades         = None
        self.buys           = None
        self.sells          = None
        self.tickers        = None
        self.prices         = None
        self.divs           = None
        self.inception_date = None

    @staticmethod
    def get_etf_buys(trades):
        etf_buys = (trades
                    .filter((pl.col('asset_type') == 'Stocks') &
                            (pl.col('quantity') > 0)))
        return etf_buys

    @staticmethod
    def get_etf_sells(trades):
        etf_sells = (trades
                     .filter((pl.col('asset_type') == 'Stocks') &
                             (pl.col('quantity') < 0)))
        return etf_sells

    @staticmethod
    def get_date_range_for_load(start_date):
        first_business_day = (start_date - BDay(1)).to_pydatetime().date()
        last_business_day = (date.today() - BDay(1)).to_pydatetime().date()
        return first_business_day, last_business_day

    def get_inception_date(self):
        self.inception_date = min(self.trades['datetime']).date()

    def get_buys_sells(self):
        self.buys = self.get_etf_buys(self.trades)
        self.sells = self.get_etf_sells(self.trades)