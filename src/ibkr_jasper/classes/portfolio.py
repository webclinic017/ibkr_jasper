import polars as pl
from datetime import timedelta

from src.ibkr_jasper.classes.portfolio_base import PortfolioBase
from src.ibkr_jasper.timer import Timer


class Portfolio(PortfolioBase):
    def __init__(self, name='', total_portfolio=None):
        super().__init__()
        self.target_weights  = {}
        self.total_portfolio = total_portfolio
        self.name            = name

    def load(self):
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

    def load_description(self):
        cur_port_path = self.PORTFOLIOS_PATH / f'{self.name}.portfolio'
        with open(cur_port_path) as file:
            lines = [line.rstrip() for line in file]

        for line in lines:
            split_line = line.split(' ')
            self.target_weights[split_line[0]] = split_line[1]

        self.tickers = list(self.target_weights.keys())

    def load_trades(self):
        self.trades = (self.total_portfolio.trades
                       .filter(pl.col('ticker').cast(pl.Utf8).is_in(self.tickers)))

    def load_divs(self):
        self.divs = (self.total_portfolio.divs
                     .filter(pl.col('ticker').cast(pl.Utf8).is_in(self.tickers)))

    def load_prices(self):
        self.prices = (self.total_portfolio.prices
                       .filter(pl.col('date') >= self.inception_date)
                       .select(['date'] + self.tickers))

    def get_port_for_date(self, date_asof):
        """Gives portfolio value on previous day close"""
        buys_asof = self.buys.filter(pl.col('datetime') < date_asof)
        sells_asof = self.sells.filter(pl.col('datetime') < date_asof)
        port_asof = {n: [0] for n in self.tickers}
        for etf in self.tickers:
            long = (buys_asof
                    .filter(pl.col('ticker') == etf)
                    .select('quantity')
                    .sum()
                    .fill_null(0)
                    .to_numpy()[0, 0])
            short = (sells_asof
                     .filter(pl.col('ticker') == etf)
                     .select('quantity')
                     .sum()
                     .fill_null(0)
                     .to_numpy()[0, 0])
            port_asof[etf] = long + short

        return port_asof

    def get_portfolio_value(self, port_asof, date_asof):
        """Gives portfolio value on previous day close prices"""
        total_value = 0
        for etf, pos in port_asof.items():
            if pos == 0:
                continue

            price = (self.prices
                     .filter(pl.col('date') < date_asof)
                     .select(pl.col(etf))
                     .reverse()
                     .limit(1)
                     .to_numpy()[0, 0])
            total_value += pos * price

        return total_value

    def get_cur_month_deals_value(self, start_date):
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        deals_cur_month = (self.buys
                           .select(['datetime', 'quantity', 'price', 'fee'])
                           .extend(self.sells
                                   .select(['datetime', 'quantity', 'price', 'fee']))
                           .filter((start_date <= pl.col('datetime')) & (pl.col('datetime') < end_date))  # TODO use between function here
                           .with_column((pl.col('quantity') * pl.col('price') - pl.col('fee')).alias('value'))
                           .select(pl.col('value'))
                           .sum()
                           .fill_null(0)
                           .to_numpy()[0, 0])

        return deals_cur_month

    def get_cur_month_divs(self, start_date):
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        divs_cur_month = (self.divs
                          .filter((start_date <= pl.col('ex-date')) & (pl.col('ex-date') < end_date))  # TODO use between function here
                          .select(pl.col('div total'))
                          .sum()
                          .fill_null(0)
                          .to_numpy()[0, 0])

        return divs_cur_month

    def get_period_return(self, start_date, end_date):
        trade_dates = (self.buys
                       .select('datetime')
                       .extend(self.sells
                               .select('datetime'))
                       .with_column(pl.col('datetime').cast(pl.Date))
                       .rename({'datetime': 'date'})
                       .extend(self.divs
                               .select('ex-date')
                               .rename({'ex-date': 'date'}))
                       .filter((start_date <= pl.col('date')) & (pl.col('date') < end_date))  # TODO use between function here
                       .unique()
                       .get_column('date')
                       .to_numpy()
                       .tolist())
        trade_dates = sorted(trade_dates + [start_date.date(), end_date.date()])

        return_total = 1
        value_prev = None
        for date in trade_dates:
            # TODO put all three in one array
            buys_today = (self.buys
                          .filter(pl.col('datetime').cast(pl.Date) == date)
                          .with_column((pl.col('quantity') * pl.col('price') - pl.col('fee')).alias('sum'))
                          .select('sum')
                          .sum()
                          .fill_null(0)
                          .to_numpy()[0, 0])
            sells_today = (self.sells
                           .filter(pl.col('datetime').cast(pl.Date) == date)
                           .with_column((pl.col('quantity') * pl.col('price') - pl.col('fee')).alias('sum'))
                           .select('sum')
                           .sum()
                           .fill_null(0)
                           .to_numpy()[0, 0])
            divs_today = (self.divs
                          .filter(pl.col('ex-date') == date)
                          .select('div total')
                          .sum()
                          .fill_null(0)
                          .to_numpy()[0, 0])
            delta_today = buys_today + sells_today - divs_today

            port_morning = self.get_port_for_date(date)
            value_morning = self.get_portfolio_value(port_morning, date)
            port_evening = self.get_port_for_date(date + timedelta(days=1))
            value_evening = self.get_portfolio_value(port_evening, date + timedelta(days=1))

            if value_prev is None:
                value_prev = value_morning

            return_prev = value_morning / value_prev if value_prev > 0 else 1
            return_today = (value_evening - delta_today) / value_morning if value_morning > 0 else 1
            return_total *= return_prev * return_today

            value_prev = value_evening

        return return_total - 1
