import dateutil.rrule as rrule
import pandas as pd
import polars as pl
from datetime import date, timedelta, datetime, time
from pandas._libs.tslibs.offsets import BDay
from pathlib import Path
from prettytable import PrettyTable


class PortfolioBase:
    PORTFOLIOS_PATH = Path('../../portfolios')

    def __init__(self) -> None:
        self.trades          = None
        self.buys            = None
        self.sells           = None
        self.tickers         = None
        self.tickers_shared  = None
        self.tickers_unique  = None
        self.prices          = None
        self.divs            = None
        self.inception_date  = None
        self.current_weights = None
        self.debug           = False

    @staticmethod
    def get_etf_buys(trades: pl.DataFrame) -> pl.DataFrame:
        etf_buys = (trades
                    .filter((pl.col('asset_type') == 'Stocks') &
                            (pl.col('quantity') > 0)))
        return etf_buys

    @staticmethod
    def get_etf_sells(trades: pl.DataFrame) -> pl.DataFrame:
        etf_sells = (trades
                     .filter((pl.col('asset_type') == 'Stocks') &
                             (pl.col('quantity') < 0)))
        return etf_sells

    @staticmethod
    def get_date_range_for_load(start_date: date) -> tuple[date, date]:
        first_business_day = (start_date - BDay(1)).to_pydatetime().date()
        last_business_day = (date.today() - BDay(1)).to_pydatetime().date()
        return first_business_day, last_business_day

    @staticmethod
    def print_df(df_pl: pl.DataFrame) -> None:
        with pd.option_context(
                'display.max_rows', None,
                'display.max_columns', None,
                'display.width', 1000,
        ):
            df_pd = df_pl.to_pandas()
            print(df_pd)

    def get_inception_date(self) -> None:
        self.inception_date = self.trades['datetime'].min().date()

    def get_buys_sells(self) -> None:
        self.buys = self.get_etf_buys(self.trades)
        self.sells = self.get_etf_sells(self.trades)

    def get_port_for_date(self, date_asof: datetime) -> dict:
        """Gives portfolio value on previous day close"""
        buys_asof = self.buys.filter(pl.col('datetime') < date_asof)
        sells_asof = self.sells.filter(pl.col('datetime') < date_asof)
        port_asof = {n: [0] for n in self.tickers}
        for etf in self.tickers:
            long = (buys_asof
                    .filter(pl.col('ticker') == etf)
                    ['quantity']
                    .append(pl.Series([0.0]))
                    .sum())
            short = (sells_asof
                    .filter(pl.col('ticker') == etf)
                    ['quantity']
                    .append(pl.Series([0.0]))
                    .sum())
            port_asof[etf] = long + short

        return port_asof

    def get_portfolio_value(self, port_asof: dict, date_asof: datetime) -> float:
        """Gives portfolio value on previous day close prices"""
        total_value = 0
        for ticker, pos in port_asof.items():
            total_value += self.get_ticker_value(ticker, pos, date_asof)
        return total_value

    def get_ticker_value(self, ticker: str, pos: int, date_asof: datetime) -> float:
        if pos == 0:
            return 0

        price = (pl.last(self.prices
                         .filter((pl.col('ticker') == ticker) &
                                 (pl.col('date') < date_asof))
                         .tail(1)
                         ['price']))
        return pos * price

    def get_cur_month_deals_value(self, start_date: datetime) -> float:
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        deals_cur_month = (self.buys
                           .select(['datetime', 'quantity', 'price', 'fee'])
                           .extend(self.sells
                                   .select(['datetime', 'quantity', 'price', 'fee']))
                           .filter(pl.col('datetime').is_between(start_date, end_date, include_bounds=(True, False)))
                           .with_column((pl.col('quantity') * pl.col('price') - pl.col('fee')).alias('value'))
                           ['value']
                           .append(pl.Series([0.0]))
                           .sum())

        return deals_cur_month

    def get_cur_month_divs(self, start_date: datetime) -> float:
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        divs_cur_month = (self.divs
                          .filter(pl.col('ex-date').is_between(start_date, end_date, include_bounds=(True, False)))
                          ['div total']
                          .append(pl.Series([0.0]))
                          .sum())

        return divs_cur_month

    def get_period_return(self, start_date: datetime, end_date: datetime) -> float:
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
        for dt in trade_dates:
            trades_today = (self.trades
                            .filter(pl.col('datetime').cast(pl.Date) == dt)
                            .with_column((pl.col('quantity') * pl.col('price') - pl.col('fee')).alias('sum'))
                            ['sum']
                            .append(pl.Series([0.0]))
                            .sum())
            divs_today = (self.divs
                          .filter(pl.col('ex-date') == dt)
                          ['div total']
                          .append(pl.Series([0.0]))
                          .sum())
            delta_today = trades_today - divs_today

            port_morning = self.get_port_for_date(dt)
            value_morning = self.get_portfolio_value(port_morning, dt)
            port_evening = self.get_port_for_date(dt + timedelta(days=1))
            value_evening = self.get_portfolio_value(port_evening, dt + timedelta(days=1))

            if value_prev is None:
                value_prev = value_morning

            return_prev = value_morning / value_prev if value_prev > 0 else 1
            return_today = (value_evening - delta_today) / value_morning if value_morning > 0 else 1
            return_total *= return_prev * return_today

            value_prev = value_evening

        return return_total - 1

    def print_report(self) -> None:
        first_report_date = self.inception_date.replace(day=1)
        cur_datetime = datetime.combine(date.today(), time())
        all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=date.today()))
        all_report_dates += [cur_datetime]

        report_table = PrettyTable()
        report_table.align = 'r'
        report_table.field_names = [''] + self.tickers + ['start', 'deals', 'divs', 'end', 'return']
        for cur_report_date in all_report_dates:
            # start portfolio value
            port_start = self.get_port_for_date(cur_report_date)
            value_start = self.get_portfolio_value(port_start, cur_report_date)

            # deals and divs
            deals_value = self.get_cur_month_deals_value(cur_report_date)
            divs = self.get_cur_month_divs(cur_report_date)

            # end portfolio value
            cur_end_date = (cur_report_date + timedelta(days=32)).replace(day=1)
            port_end = self.get_port_for_date(cur_end_date)
            end_value = self.get_portfolio_value(port_end, cur_end_date)

            # return in percents
            ret = self.get_period_return(cur_report_date, cur_end_date)

            report_table.add_row([cur_report_date.date()] +
                                 [f'{port_start[x]:.0f}' for x in self.tickers] +
                                 [f'{value_start:.2f}'] +
                                 [f'{deals_value:.2f}'] +
                                 [f'{divs:.2f}'] +
                                 [f'{end_value:.2f}'] +
                                 [f'{100 * ret:.2f}%'])
            if cur_report_date.month == 12 and (cur_datetime.month != 12 or cur_datetime.year != cur_report_date.year):
                report_table.add_row([''] * len(report_table.field_names))

        print(report_table)
