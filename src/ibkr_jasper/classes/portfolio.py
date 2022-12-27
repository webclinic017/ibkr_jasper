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
        with Timer(f'Load portfolio description for {self.name}', True):
            self.load_description()
        with Timer(f'Load trades for {self.name}', True):
            self.load_trades()
        with Timer('Split trades on buys & sells', True):
            self.get_buys_sells()
        with Timer(f'Load divs for {self.name}', True):
            self.load_divs()
        with Timer('Get total portfolio start date', True):
            self.get_inception_date()
        with Timer(f'Load prices for {self.name}', True):
            self.load_prices()

        return self

    def load_description(self) -> None:
        cur_port_path = self.PORTFOLIOS_PATH / f'{self.name}.portfolio'
        with open(cur_port_path) as file:
            lines = [line.rstrip() for line in file]

        for line in lines:
            split_line = line.split(' ')
            self.target_weights[split_line[0]] = split_line[1]

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
                       .filter(pl.col('date') >= self.inception_date)
                       .select(['date'] + self.tickers))
