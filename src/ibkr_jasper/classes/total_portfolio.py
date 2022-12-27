from __future__ import annotations
import csv
import numpy as np
import pandas as pd
import pickle
import polars as pl
import yfinance as yf
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from pandas._libs.tslibs.offsets import BDay

from src.ibkr_jasper.classes.portfolio_base import PortfolioBase
from src.ibkr_jasper.timer import Timer


class TotalPortfolio(PortfolioBase):
    DATA_PATH             = Path('../../data')
    PRICES_PICKLE_PATH    = DATA_PATH / 'prices.pickle'
    SPLITS_PICKLE_PATH    = DATA_PATH / 'splits.pickle'
    XRUB_PICKLE_PATH      = DATA_PATH / 'xrub.pickle'
    SHARED_TICKERS_TRADES = PortfolioBase.PORTFOLIOS_PATH / 'shared_tickers.deals'

    def __init__(self) -> None:
        super().__init__()
        self.report_list     = []
        self.io              = None
        self.splits          = None
        self.xrub_rates      = None
        self.tlh_trades      = None
        self.shared_trades   = None
        self.tickers_mapping = {}
        self.all_portfolios  = {}

    def load(self) -> TotalPortfolio:
        with Timer('Read reports', True):
            self.load_raw_reports()
        with Timer('Parse deposits & withdrawals', True):
            self.fetch_io()
        with Timer('Parse trades', True):
            self.fetch_trades()
        with Timer('Parse dividends', True):
            self.fetch_divs()
        with Timer('Get all tickers in total portfolio', True):
            self.get_all_tickers()
        with Timer('Load all portfolios', True):
            self.load_all_portfolios()
        with Timer('Get shared tickers in total portfolio', True):
            self.get_shared_tickers()
        with Timer('Get total portfolio start date', True):
            self.get_inception_date()
        with Timer('Loading of ETF prices and splits', True):
            self.load_prices_and_splits()
        with Timer('Loading of Central bank exchange rates prices', True):
            self.load_xrub_rates()
        with Timer('Adjust trades by splits', True):
            self.adjust_trades_by_splits()
        with Timer('Distribute trades', True):
            self.distribute_trades()
        with Timer('Split trades on buys & sells', True):
            self.get_buys_sells()
        with Timer('Get trades for tax loss harvesting', True):
            self.get_tlh_trades()

        return self

    def load_raw_reports(self) -> None:
        report_files = [x for x in self.DATA_PATH.glob('**/*') if x.is_file() and x.suffix == '.csv']
        for report_file in report_files:
            report_reader = csv.reader(open(report_file), delimiter=',')
            self.report_list += list(report_reader)

    def fetch_io(self) -> None:
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

    def fetch_divs(self) -> None:
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

    def fetch_trades(self) -> None:
        trades_report = [x for x in self.report_list if x[0] == 'Trades']
        trades_columns = ['datetime', 'ticker', 'quantity', 'price', 'curr', 'fee', 'asset_type', 'code']
        trades_data = {x: [] for x in trades_columns}
        for row in iter(trades_report):
            if row[1] != 'Data' or row[2] != 'Trade':
                continue
            trades_data['datetime'].append(datetime.fromisoformat(row[6].replace(',', '')))
            trades_data['ticker'].append(row[5])
            trades_data['quantity'].append(float(row[8].replace(',', '')))
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

    def get_all_tickers(self) -> None:
        all_tickers = self.trades['ticker'].unique().to_list()
        self.tickers = [x for x in all_tickers if not '.' in x]

    def load_all_portfolios(self) -> None:
        port_paths = [x for x in self.PORTFOLIOS_PATH.glob('**/*') if x.is_file() and x.suffix == '.portfolio']
        for cur_port_path in port_paths:
            port_name = cur_port_path.stem
            with open(cur_port_path) as file:
                lines = [line.rstrip() for line in file if not line.startswith('#')]

            target_weights = {}
            for line in lines:
                split_line = line.split(' ')
                if len(split_line) != 2:
                    continue
                ticker = split_line[0]
                weight = float(split_line[1])
                target_weights[ticker] = weight
                if not ticker in self.tickers_mapping:
                    self.tickers_mapping[ticker] = set()
                self.tickers_mapping[ticker].add(port_name)

            if not target_weights:
                continue
            assert sum(target_weights.values()) == 100, 'Sum of targets weights should be 100'
            self.all_portfolios[port_name] = target_weights

    def get_shared_tickers(self) -> None:
        all_portfolios = [x for x in self.PORTFOLIOS_PATH.glob('**/*') if x.is_file() and x.suffix == '.portfolio']
        all_tickers = []
        for cur_port_path in all_portfolios:
            with open(cur_port_path) as file:
                lines = [line.rstrip() for line in file if not line.startswith('#')]
                for line in lines:
                    all_tickers.append(line.split(' ')[0])
        self.tickers_shared = [k for k, v in Counter(all_tickers).items() if v > 1]
        self.tickers_unique = list(set(self.tickers).difference(self.tickers_shared))

    def distribute_trades(self) -> None:
        # load shared trades mapping
        shared_trades_list = []
        with open(self.SHARED_TICKERS_TRADES) as file:
            lines = [line.rstrip() for line in file if not line.startswith('#')]
            for line in lines:
                data_list = [x for x in line.split(' ') if x]
                data_dict = {
                    'date': datetime.strptime(data_list[0], "%Y.%m.%d").date(),
                    'ticker': data_list[1],
                    'quantity': int(data_list[2]),
                    'portfolio': data_list[3],
                    'type': data_list[4] if len(data_list) == 5 else 'REAL',
                }
                shared_trades_list.append(data_dict)
        self.shared_trades = pl.from_dicts(shared_trades_list).with_column(pl.col('ticker').cast(pl.Categorical))

        # unique trades
        trades_unique = (self.trades
                         .filter(pl.col('ticker').cast(pl.Utf8).is_in(self.tickers_unique))
                         .with_column(pl.col('ticker')
                                      .cast(pl.Utf8)
                                      .apply(lambda x: next(iter(self.tickers_mapping[x])))
                                      .alias('portfolio')))

        # shared trades
        w = (self.trades
             .filter(pl.col('ticker').cast(pl.Utf8).is_in(self.tickers_shared))
             .with_columns([pl.col('datetime').cast(pl.Date).alias('date'),
                            pl.col('fee') / pl.col('quantity'),
                            pl.col('quantity').apply(lambda x: [np.sign(x)] * abs(int(x)))])
             .explode('quantity')
             .sort(['date', 'ticker', 'price']))
        q = (self.shared_trades
             .filter(pl.col('type') == 'REAL')
             .drop('type')
             .with_column(pl.col('quantity').cast(pl.Float64).apply(lambda x: [np.sign(x)] * abs(int(x))))
             .explode('quantity')
             .sort(['date', 'ticker']))

        # checks
        wq_anti = w.join(q, on=['date', 'ticker', 'quantity'], how='anti')
        qw_anti = q.join(q, on=['date', 'ticker', 'quantity'], how='anti')
        if len(wq_anti):
            wq_anti = (wq_anti
                       .drop(['fee', 'asset_type', 'code', 'date'])
                       .groupby(['datetime', 'ticker', 'price', 'curr'])
                       .agg(pl.col('quantity').sum())
                       .sort('datetime'))
            print('These trades on shared tickers that are not mapped:')
            self.print_df(wq_anti)
        if len(qw_anti):
            qw_anti = (qw_anti
                       .groupby(['date', 'portfolio', 'ticker'])
                       .agg(pl.col('quantity').sum())
                       .sort('date'))
            print('These are mapped trades on shared tickers that do not exist:')
            self.print_df(qw_anti)
        assert len(wq_anti) == 0, 'There are trades on shared tickers that are not mapped'
        assert len(qw_anti) == 0, 'There are mapped trades on shared tickers that do not exist'
        assert len(w) == len(q)

        w_new = (w
                 .hstack(q.rename({'date': 'date_r', 'quantity': 'quantity_r', 'ticker': 'ticker_r'}))
                 .with_column((pl.when((pl.col('date') != pl.col('date_r')) &
                                       (pl.col('ticker') != pl.col('ticker_r')) &
                                       (pl.col('quantity') != pl.col('quantity_r')))
                               .then(pl.lit(1))
                               .otherwise(pl.lit(0)))
                              .alias('errors'))
                 .drop(['date', 'date_r', 'ticker_r', 'quantity_r'])
                 .groupby(['datetime', 'ticker', 'price', 'curr', 'asset_type', 'code', 'portfolio'])
                 .agg(pl.col(['quantity', 'fee', 'errors']).sum()))
        assert w_new['errors'].sum() == 0
        trades_shared = w_new.drop('errors')

        # virtual trades
        trades_virtual = (self.shared_trades
                          .filter(pl.col('type') == 'VIRTUAL')
                          .with_columns([pl.col('quantity').cast(pl.Float64),
                                         pl.col('date').cast(pl.Datetime).alias('datetime'),
                                         pl.lit('USD').cast(pl.Categorical).alias('curr'),
                                         pl.lit(0.0).alias('fee'),
                                         pl.lit('Stocks').cast(pl.Categorical).alias('asset_type'),
                                         pl.lit('V').alias('code')])
                          .join(self.prices, on=['date', 'ticker'], how='left')
                          .drop(['date', 'type']))

        self.trades = (pl.concat([trades_unique, trades_shared, trades_virtual], how='diagonal')
                       .sort(['datetime', 'ticker', 'portfolio']))

    def load_prices_and_splits(self) -> None:
        first_business_day, last_business_day = self.get_date_range_for_load(self.inception_date)

        # try to load cache data and if something is missing, then reload all prices
        try:
            with open(self.PRICES_PICKLE_PATH, 'rb') as handle:
                self.prices = pickle.load(handle)
            with open(self.SPLITS_PICKLE_PATH, 'rb') as handle:
                self.splits = pickle.load(handle)

            saved_min_date = self.prices['date'].min()
            saved_max_date = self.prices['date'].max()
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
                           .melt(id_vars='date', variable_name='ticker', value_name='price')
                           .with_columns([pl.col('date').cast(pl.Date),
                                          pl.col('ticker').cast(pl.Categorical)]))
            splits_today = (pl.DataFrame({'datetime': [date.today() + timedelta(days=1)] * len(self.tickers),
                                          'ticker': self.tickers,
                                          'splits': [1.0] * len(self.tickers)})
                            .with_columns([pl.col('datetime').cast(pl.Datetime),
                                           pl.col('ticker').cast(pl.Categorical)]))
            splits_hist = (pl.from_pandas(data['Stock Splits'].reset_index())
                           .rename({'Date': 'datetime'})
                           .melt(id_vars='datetime', variable_name='ticker', value_name='splits')
                           .filter(pl.col('splits') > 0)
                           .with_columns([pl.col('datetime').cast(pl.Datetime),
                                          pl.col('ticker').cast(pl.Categorical)])
                           .vstack(splits_today))

            # do not know how to optimize it
            # TODO use partition_by
            splits_list = []
            for ticker in self.tickers:
                cur_splits = (splits_hist
                              .filter(pl.col('ticker') == ticker)
                              .with_column(pl.col('splits').cumprod(reverse=True).alias('coef'))
                              .drop('splits'))
                splits_list.append(cur_splits)

            self.splits = pl.concat(splits_list)

        with open(self.PRICES_PICKLE_PATH, 'wb') as handle:
            pickle.dump(self.prices, handle, protocol=pickle.HIGHEST_PROTOCOL)
        with open(self.SPLITS_PICKLE_PATH, 'wb') as handle:
            pickle.dump(self.splits, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def load_xrub_rates(self) -> None:
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
            xrub_rates = (pl.DataFrame(pd.read_xml(url))
                          .drop('Nominal')
                          .rename({'Date': 'date', 'Id': 'curr', 'Value': 'rate'})
                          .with_columns([pl.col('date').str.strptime(pl.Date, fmt='%d.%m.%Y').cast(pl.Date),
                                         pl.col('curr').str.replace('R01235', 'USD').cast(pl.Categorical),
                                         pl.col('rate').str.replace(',', '.').cast(pl.Float64)]))
            dates_list = [first_business_day + timedelta(days=x) for x in
                          range((last_business_day - first_business_day).days + 1)]
            self.xrub_rates = (pl.DataFrame(dates_list, columns=['date'])
                                 .with_column(pl.col('date').cast(pl.Date))
                                 .join(xrub_rates, on='date', how='left')
                                 .with_column(pl.col(['curr', 'rate']).backward_fill()))

        with open(self.XRUB_PICKLE_PATH, 'wb') as handle:
            pickle.dump(self.xrub_rates, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def adjust_trades_by_splits(self) -> None:
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

    def get_tlh_trades(self) -> None:
        """
        Tax Loss Harvesting
        """
        filtered_trades = []
        for ticker in self.tickers:
            cur_price = pl.last(self.prices.filter(pl.col('ticker') == ticker)['price'])
            # TODO switch after polars update
            # cur_price = self.prices.filter(pl.col('ticker') == ticker).select('price').tail(1).item()
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
