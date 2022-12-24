import csv
import numpy as np
import pandas as pd
import pickle
import polars as pl
import yfinance as yf
from datetime import date, datetime, timedelta
from pathlib import Path
from pandas._libs.tslibs.offsets import BDay

from src.ibkr_jasper.classes.portfolio_base import PortfolioBase
from src.ibkr_jasper.timer import Timer


class TotalPortfolio(PortfolioBase):
    DATA_PATH          = Path('../../data')
    PRICES_PICKLE_PATH = DATA_PATH / 'prices.pickle'
    SPLITS_PICKLE_PATH = DATA_PATH / 'splits.pickle'
    XRUB_PICKLE_PATH   = DATA_PATH / 'xrub.pickle'

    def __init__(self):
        super().__init__()
        self.report_list   = []
        self.io            = None
        self.splits        = None
        self.xrub_rates    = None
        self.tlh_trades    = None

    def load(self):
        with Timer('Read reports', True):
            self.load_raw_reports()
        with Timer('Parse deposits & withdrawals', True):
            self.fetch_io()
        with Timer('Parse trades', True):
            self.fetch_trades()
        with Timer('Parse dividends', True):
            self.fetch_divs()
        with Timer('Get all tickers in total portfolio', True):
            self.get_all_etfs()
        with Timer('Get total portfolio start date', True):
            self.get_inception_date()
        with Timer('Loading of ETF prices and splits', True):
            self.load_prices_and_splits()
        with Timer('Loading of Central bank exchange rates prices', True):
            self.load_xrub_rates()
        with Timer('Adjust trades by splits', True):
            self.adjust_trades_by_splits()
        with Timer('Split trades on buys & sells', True):
            self.get_buys_sells()
        with Timer('Get trades for tax loss harvesting', True):
            self.get_tlh_trades()

        return self

    def load_raw_reports(self):
        all_files = [x for x in self.DATA_PATH.glob('**/*') if x.is_file()]
        report_files = [x for x in all_files if x.suffix == '.csv']

        for report_file in report_files:
            report_reader = csv.reader(open(report_file), delimiter=',')
            self.report_list += list(report_reader)

    def fetch_io(self):
        io_report = [x for x in self.report_list if x[0] == 'Deposits & Withdrawals']
        io_columns = ['date', 'curr', 'amount', 'desc']
        io_currencies = {x[2] for x in io_report if len(x[2]) == 3}
        io_data = {x: [] for x in io_columns}
        for row in iter(io_report):
            if row[2] not in io_currencies:
                continue
            io_data['date'].append(date.fromisoformat(row[3]))
            io_data['curr'].append(row[2])
            io_data['amount'].append(float(row[5]))
            io_data['desc'].append(row[4])
        self.io = (pl.DataFrame(io_data)
                   .with_column(pl.col('curr').cast(pl.Categorical))
                   .unique()
                   .sort(by=io_columns))

    def fetch_divs(self):
        # mb simpler to load them from yahoo finance
        # parse small divs table
        # TODO remove this table, because it has much less information
        divs_report = [x for x in self.report_list if x[0] == 'Dividends']
        divs_currencies = {x[2] for x in divs_report if len(x[2]) == 3}
        divs_columns = ['pay date', 'ticker', 'div total', 'curr']
        divs_data = {x: [] for x in divs_columns}
        for row in iter(divs_report):
            if row[2] not in divs_currencies:
                continue
            divs_data['pay date'].append(date.fromisoformat(row[3]))
            divs_data['ticker'].append(row[4].split('(')[0].replace(' ', ''))
            divs_data['div total'].append(float(row[5]))
            divs_data['curr'].append(row[2])
        divs_df = (pl.DataFrame(divs_data)
                   .with_columns([
                        pl.col('ticker').cast(pl.Categorical),
                        pl.col('curr').cast(pl.Categorical),
                    ])
                   .unique()
                   .sort(by=['pay date', 'ticker'])
                   .groupby(['pay date', 'ticker', 'curr'], maintain_order=True)
                   .sum()
                   .select(divs_columns))

        # parse big divs table
        accruals_report = [x for x in self.report_list if x[0] == 'Change in Dividend Accruals']
        accruals_columns = ['ex-date', 'pay date', 'ticker', 'quantity', 'div per share', 'div total', 'curr', 'tax']
        accruals_data = {x: [] for x in accruals_columns}
        for row in iter(accruals_report):
            if row[-1] != 'Re':
                continue
            accruals_data['ex-date'].append(date.fromisoformat(row[6]))
            accruals_data['pay date'].append(date(1900, 1, 1) if row[7] == '-' else date.fromisoformat(row[7]))
            accruals_data['ticker'].append(row[4])
            accruals_data['quantity'].append(int(row[8]))
            accruals_data['curr'].append(row[3])
            accruals_data['div per share'].append(float(row[11]))
            accruals_data['div total'].append(-float(row[12]))
            accruals_data['tax'].append(float(row[9]))
        self.divs = (pl.DataFrame(accruals_data)
                     .with_columns([
                                 pl.col('ticker').cast(pl.Categorical),
                                 pl.col('curr').cast(pl.Categorical),
                             ])
                     .unique()
                     .groupby(['ex-date', 'ticker', 'quantity', 'div per share', 'curr'], maintain_order=True)
                     .last()
                     .select(accruals_columns)
                     .sort(by=['ex-date', 'ticker']))

    def fetch_trades(self):
        trades_report = [x for x in self.report_list if x[0] == 'Trades']
        trades_columns = ['datetime', 'ticker', 'quantity', 'price', 'curr', 'fee', 'asset_type', 'code']
        trades_data = {x: [] for x in trades_columns}
        for row in iter(trades_report):
            if row[1] != 'Data' or row[2] != 'Trade':
                continue
            trades_data['datetime'].append(datetime.fromisoformat(row[6].replace(',', '')))
            trades_data['ticker'].append(row[5])
            trades_data['quantity'].append(float(row[8].replace(',', '.')))
            trades_data['price'].append(float(row[9]))
            trades_data['curr'].append(row[4])
            trades_data['fee'].append(float(row[12]) if row[12] != '' else 0.0)
            trades_data['asset_type'].append(row[3])
            trades_data['code'].append(row[16])

        self.trades = (pl.DataFrame(trades_data)
                       .with_columns([
                               pl.col('ticker').cast(pl.Categorical),
                               pl.col('curr').cast(pl.Categorical),
                               pl.col('asset_type').cast(pl.Categorical),
                           ])
                       .sort(by=['datetime', 'ticker']))

    def get_all_etfs(self):
        all_tickers = np.transpose(self.trades.select('ticker').unique().to_numpy()).tolist()[0]
        self.tickers = [x for x in all_tickers if len(x) <= 4]

    def load_prices_and_splits(self):
        first_business_day, last_business_day = self.get_date_range_for_load(self.inception_date)

        # try to load cache data and if something is missing, then reload all prices
        try:
            with open(self.PRICES_PICKLE_PATH, 'rb') as handle:
                self.prices = pickle.load(handle)
            with open(self.SPLITS_PICKLE_PATH, 'rb') as handle:
                self.splits = pickle.load(handle)

            saved_min_date = (self.prices
                              .select(pl.col('date').min())
                              .to_struct('')[0]['date'])
            saved_max_date = (self.prices
                              .select(pl.col('date').max())
                              .to_struct('')[0]['date'])
            saved_etfs = self.prices.columns[1:]

            if (set(saved_etfs) == set(self.tickers) and
                    saved_min_date == first_business_day and
                    saved_max_date == last_business_day):
                return
            else:
                print('Cache file with prices misses some values')
        except FileNotFoundError:
            print('Cache file with prices or splits does not exist')

        with Timer('Full reload of prices from yahoo', True):
            data = yf.download(self.tickers, start=first_business_day, end=last_business_day + BDay(1), actions=True)
            self.prices = (pl.from_pandas(data['Close'].reset_index())
                        .rename({'Date': 'date'})
                        .with_column(pl.col('date').cast(pl.Date)))
            today_splits = (pl.DataFrame({'datetime': [date.today() + timedelta(days=1)] * len(self.tickers),
                                          'ticker': self.tickers,
                                          'splits': [1.0] * len(self.tickers)})
                            .with_column(pl.col('datetime').cast(pl.Datetime)))
            self.splits = (pl.from_pandas(data['Stock Splits'].reset_index())
                        .rename({'Date': 'datetime'})
                        .melt(id_vars='datetime', variable_name='ticker', value_name='splits')
                        .filter(pl.col('splits') > 0)
                        .with_column(pl.col('datetime').cast(pl.Datetime))
                        .vstack(today_splits))
            splits_list = []

            # do not know how to optimize it
            # TODO use partition_by
            for ticker in self.tickers:
                cur_splits = (self.splits
                              .filter(pl.col('ticker') == ticker)
                              .with_columns([pl.col('ticker').cast(pl.Categorical),
                                             pl.col('splits').cumprod(reverse=True).alias('coef')])
                              .drop('splits'))
                splits_list.append(cur_splits)

            self.splits = pl.concat(splits_list)

        with open(self.PRICES_PICKLE_PATH, 'wb') as handle:
            pickle.dump(self.prices, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open(self.SPLITS_PICKLE_PATH, 'wb') as handle:
            pickle.dump(self.splits, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def load_xrub_rates(self):
        """
        Central bank publishes exchange rates for Tuesdays to Saturdays, so some tricks should be applied
        """
        first_business_day, last_business_day = self.get_date_range_for_load(self.inception_date)
        s = first_business_day.strftime('%d/%m/%Y')
        e = last_business_day.strftime('%d/%m/%Y')

        # try to load cache data and if something is missing, then reload all rates
        try:
            with open(self.XRUB_PICKLE_PATH, 'rb') as handle:
                self.xrub_rates = pickle.load(handle)

            saved_min_date = self.xrub_rates['date'].min()
            saved_max_date = self.xrub_rates['date'].max()

            if (saved_min_date == first_business_day and
                    saved_max_date == last_business_day):
                return self.xrub_rates
            else:
                print('Cache file with prices misses some values')
        except FileNotFoundError:
            print('Cache file with rates does not exist')

        with Timer('Full reload of prices from Central Bank API', True):
            url = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={s}&date_req2={e}&VAL_NM_RQ=R01235'
            self.xrub_rates = (pl.DataFrame(pd.read_xml(url))
                                 .drop('Nominal')
                                 .rename({'Date': 'date', 'Id': 'curr', 'Value': 'rate'})
                                 .with_columns([pl.col('date').str.strptime(pl.Date, fmt='%d.%m.%Y').cast(pl.Date),
                                                pl.col('curr').str.replace('R01235', 'USD').cast(pl.Categorical),
                                                pl.col('rate').str.replace(',', '.').cast(pl.Float32)]))
            dates_list = [first_business_day + timedelta(days=x) for x in
                          range((last_business_day - first_business_day).days + 1)]
            self.xrub_rates = (pl.DataFrame(dates_list, columns=['date'])
                                 .with_column(pl.col('date').cast(pl.Date))
                                 .join(self.xrub_rates, on='date', how='left')
                                 .with_column(pl.col(['curr', 'rate']).backward_fill()))

        with open(self.XRUB_PICKLE_PATH, 'wb') as handle:
            pickle.dump(self.xrub_rates, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def adjust_trades_by_splits(self):
        trades_total_adj = []
        for ticker in self.tickers:
            cur_trades_adj = (self.trades
                              .filter(pl.col('ticker') == ticker)
                              .join_asof(self.splits
                                         .filter(pl.col('ticker') == ticker)
                                         .drop('ticker'),
                                         on='datetime', strategy='forward')
                              .with_columns([(pl.col('quantity') * pl.col('coef')).alias('quantity'),
                                             (pl.col('price') / pl.col('coef')).alias('price')])
                              .drop('coef'))
            trades_total_adj.append(cur_trades_adj)

        self.trades = pl.concat(trades_total_adj)

    def get_tlh_trades(self):
        """
        Tax Loss Harvesting
        """
        filtered_trades = []
        for ticker in self.tickers:
            cur_price = pl.last(self.prices[ticker])
            cur_trades = self.trades.filter(pl.col('ticker') == ticker)
            cur_buys = (cur_trades
                        .filter(pl.col('quantity') > 0)
                        .with_columns([
                            pl.col('datetime').cast(pl.Date).alias('date'),
                            pl.lit(cur_price).alias('cur_price'),
                            pl.min([0, cur_price - pl.col('price')]).alias('diff'),
                        ])
                        .join(self.xrub_rates, on=['date', 'curr'])
                        .drop(['datetime', 'asset_type', 'code', 'curr'])
                        .with_columns([
                            (pl.col('price') * pl.col('rate')).alias('price_rub'),
                            (pl.col('cur_price') * pl.col('rate')).alias('cur_price_rub'),
                        ])
                        .with_column((pl.col('quantity') * pl.min([0, pl.col('cur_price_rub') - pl.col('price_rub')])).alias('diff_rub')))
            cur_sells_sum = (cur_trades
                             .filter(pl.col('quantity') < 0)
                             .drop(['asset_type', 'code'])
                             ['quantity']
                             .sum())
            cur_sells_sum = 0 if cur_sells_sum is None else cur_sells_sum
            cur_buys_sum = cur_buys['quantity'].sum()
            if not (cur_sells_sum is None) and (cur_buys_sum + cur_sells_sum == 0):
                continue
            if cur_buys['diff'].sum() == 0:
                continue

            for row in cur_buys.to_dicts():
                if cur_sells_sum == 0:
                    pass
                elif cur_sells_sum + row['quantity'] < 0:
                    cur_sells_sum += row['quantity']
                    row['quantity'] = 0
                else:
                    row['quantity'] += cur_sells_sum
                    cur_sells_sum = 0

                filtered_trades.append(row)

        self.tlh_trades = (pl.from_dicts(filtered_trades)
                             .select(cur_buys.columns)
                             .filter((pl.col('quantity') > 0) & (pl.col('diff') < 0))
                             .sort('diff_rub'))