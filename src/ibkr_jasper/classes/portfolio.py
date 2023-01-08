from __future__ import annotations
import polars as pl
from datetime import datetime
from prettytable import PrettyTable

from src.ibkr_jasper.classes.portfolio_base import PortfolioBase
from src.ibkr_jasper.timer import Timer


class Portfolio(PortfolioBase):
    def __init__(self, name='', total_portfolio=None):
        super().__init__()
        self.target_weights  = None
        self.target_value    = 0
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
        with Timer(f'Calculate current weights for {self.name}', self.debug):
            self.calc_current_weights()

        return self

    def load_target_weights(self) -> None:
        self.target_weights = self.total_portfolio.all_portfolios[self.name]
        self.target_value = self.total_portfolio.all_target_values[self.name]

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

    def calc_current_weights(self) -> None:
        dt = datetime.today()
        port_latest = self.get_port_for_date(dt)
        self.current_weights = dict()
        for ticker, pos in port_latest.items():
            value = self.get_ticker_value(ticker, pos, dt)
            self.current_weights[ticker] = value / self.target_value * 100

    def print_weights(self) -> None:
        port_latest = self.get_port_for_date(datetime.today())
        total_value = self.get_portfolio_value(port_latest, datetime.today())
        print(f'${total_value:,.0f} --- current portfolio value')
        print(f'${self.target_value:,.0f} --- target portfolio value')

        weights_table = PrettyTable()
        weights_table.align = 'r'
        weights_table.field_names = ['ticker', 'target', 'fact', 'diff', '',
                                     'price', 'tgt value', 'cur value', 'lots to buy']
        for ticker in self.tickers:
            target_weight = self.target_weights[ticker]
            current_weight = self.current_weights[ticker]
            if not target_weight and not current_weight:
                continue

            diff_weight = target_weight - current_weight
            cur_price = self.get_ticker_price_last(ticker)
            tgt_value = self.target_weights[ticker] * self.target_value / 100
            cur_value = self.current_weights[ticker] * self.target_value / 100
            lots_to_buy = (tgt_value - cur_value) / cur_price
            weights_table.add_row([
                ticker,
                f'{target_weight:.0f}%',
                f'{current_weight:.1f}%',
                f'{diff_weight:.1f}%',
                '',
                f'{cur_price:.2f}',
                f'${tgt_value:,.0f}',
                f'${cur_value:,.0f}',
                f'{lots_to_buy:,.0f}',
            ])

        print(weights_table)
