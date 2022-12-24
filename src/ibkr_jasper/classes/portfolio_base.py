import dateutil.rrule as rrule
import pandas as pd
import polars as pl
from datetime import date, timedelta
from pandas._libs.tslibs.offsets import BDay
from pathlib import Path
from prettytable import PrettyTable


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

    @staticmethod
    def print_df(df_pl):
        with pd.option_context(
                'display.max_rows', None,
                'display.max_columns', None,
                'display.width', 1000,
        ):
            df_pd = df_pl.to_pandas()
            print(df_pd)

    def get_inception_date(self):
        self.inception_date = min(self.trades['datetime']).date()

    def get_buys_sells(self):
        self.buys = self.get_etf_buys(self.trades)
        self.sells = self.get_etf_sells(self.trades)

    def print_report(self):
        first_report_date = self.inception_date.replace(day=1)
        all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=date.today()))

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
            if cur_report_date.month == 12:
                report_table.add_row([''] * len(report_table.field_names))

        print(report_table)
