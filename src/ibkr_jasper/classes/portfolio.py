from __future__ import annotations
import polars as pl
from src.ibkr_jasper.classes.portfolio_base import PortfolioBase
from src.ibkr_jasper.timer import Timer


class Portfolio(PortfolioBase):
    def __init__(self, name='', total_portfolio=None):
        super().__init__()
        self.target_weights  = {}
        self.total_portfolio = total_portfolio
        self.name            = name

    def load(self) -> Portfolio:
        with Timer(f'Load target weights for {self.name}', self.debug):
            self.load_target_weights()
        with Timer(f'Load tickers for {self.name}', self.debug):
            self.load_tickers()
        with Timer(f'Load trades for {self.name}', self.debug):
            self.load_trades()
        with Timer('Split trades on buys & sells', self.debug):
            self.get_buys_sells()
        with Timer(f'Load divs for {self.name}', self.debug):
            self.load_divs()
        with Timer('Get portfolio start date', self.debug):
            self.get_inception_date()
        with Timer(f'Load prices for {self.name}', self.debug):
            self.load_prices()

        return self

    def load_target_weights(self) -> None:
        self.target_weights = self.total_portfolio.all_portfolios[self.name]

    def load_tickers(self):
        self.tickers = list(self.target_weights.keys())
        self.tickers_shared = list(set(self.total_portfolio.tickers_shared).intersection(self.tickers))
        self.tickers_unique = list(set(self.tickers).difference(self.tickers_shared))

    def load_trades(self) -> None:
        self.trades = (self.total_portfolio.trades
                       .filter(pl.col('portfolio') == self.name)
                       .drop('portfolio'))

    def load_divs(self) -> None:
        # TODO this is wrong, need to load divs afterwards from yahoo
        self.divs = (self.total_portfolio.divs
                     .filter(pl.col('ticker').cast(pl.Utf8).is_in(self.tickers)))

    def load_prices(self) -> None:
        self.prices = (self.total_portfolio.prices
                       .filter((pl.col('ticker').is_in(self.tickers)) &
                               (pl.col('date') >= self.inception_date)))
