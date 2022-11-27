import polars as pl
import csv
from pathlib import Path
from datetime import date, datetime


def load_raw_reports():
    data_path = Path('../../data')
    all_files = [x for x in data_path.glob('**/*') if x.is_file()]
    report_files = [x for x in all_files if x.suffix == '.csv']

    report_list = []
    for report_file in report_files:
        report_reader = csv.reader(open(report_file), delimiter=',')
        report_list += list(report_reader)

    return report_list


def fetch_io(report_list):
    io_report = [x for x in report_list if x[0] == 'Deposits & Withdrawals']
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
    df_io = (pl.DataFrame(io_data)
             .with_column(pl.col('curr').cast(pl.Categorical))
             .unique()
             .sort(by=io_columns))

    return df_io


def fetch_divs(report_list):
    # parse small divs table
    # TODO remove this table, because it has much less information
    divs_report = [x for x in report_list if x[0] == 'Dividends']
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
    accruals_report = [x for x in report_list if x[0] == 'Change in Dividend Accruals']
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
    accruals_df = (pl.DataFrame(accruals_data)
                   .with_columns([
                        pl.col('ticker').cast(pl.Categorical),
                        pl.col('curr').cast(pl.Categorical),
                    ])
                   .unique()
                   .groupby(['ex-date', 'ticker', 'quantity', 'div per share', 'curr'], maintain_order=True)
                   .last()
                   .select(accruals_columns)
                   .sort(by=['ex-date', 'ticker']))

    return accruals_df


def fetch_trades(report_list):
    trades_report = [x for x in report_list if x[0] == 'Trades']
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

    df_trades = (pl.DataFrame(trades_data)
                 .with_columns([
                      pl.col('ticker').cast(pl.Categorical),
                      pl.col('curr').cast(pl.Categorical),
                      pl.col('asset_type').cast(pl.Categorical),
                  ])
                 .sort(by=['datetime', 'ticker']))

    return df_trades
