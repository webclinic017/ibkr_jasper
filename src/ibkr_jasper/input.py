import polars as pl
import csv
from pathlib import Path
from datetime import date, datetime


def load_raw_reports():
    global report_list
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
    io_columns = io_report[0][2:]
    io_columns_order = [1, 0, 3, 2]
    io_columns = [io_columns[i] for i in io_columns_order]
    io_data = io_report[1:]
    io_currencies = {x[2] for x in io_data if len(x[2]) == 3}
    io_data_filtered = {k: [] for k in io_columns}
    for row in iter(io_data):
        if row[2] not in io_currencies:
            continue

        # row[3] - date
        # row[2] - currency code
        # row[5] - amount
        # row[4] - comment
        io_data_filtered[io_columns[0]].append(date.fromisoformat(row[3]))
        io_data_filtered[io_columns[1]].append(row[2])
        io_data_filtered[io_columns[2]].append(row[5])
        io_data_filtered[io_columns[3]].append(row[4])
    df_io = pl.DataFrame(io_data_filtered).with_columns([
        pl.col('Currency').cast(pl.Categorical),
        pl.col('Amount').cast(pl.Float64),
    ])
    df_io = df_io.sort(by=df_io.columns).unique()

    return df_io


def fetch_divs(report_list):
    divs_report = [x for x in report_list if x[0] == 'Dividends']
    divs_columns = divs_report[0][2:]
    divs_columns_order = [1, 0, 3, 2]
    divs_columns = [divs_columns[i] for i in divs_columns_order]
    divs_data = divs_report[1:]
    divs_currencies = {x[2] for x in divs_data if len(x[2]) == 3}
    divs_data_filtered = {k: [] for k in divs_columns}
    for row in iter(divs_data):
        if row[2] not in divs_currencies:
            continue

        # row[3] - date
        # row[2] - currency code
        # row[5] - amount
        # row[4] - comment
        divs_data_filtered[divs_columns[0]].append(date.fromisoformat(row[3]))
        divs_data_filtered[divs_columns[1]].append(row[2])
        divs_data_filtered[divs_columns[2]].append(row[5])
        divs_data_filtered[divs_columns[3]].append(row[4])
    df_divs = pl.DataFrame(divs_data_filtered).with_columns([
        pl.col('Currency').cast(pl.Categorical),
        pl.col('Amount').cast(pl.Float64),
    ])
    # TODO split description into separate columns
    df_divs = df_divs.sort(by=df_divs.columns).unique()

    return df_divs


def fetch_trades(report_list):
    trades_report = [x for x in report_list if x[0] == 'Trades']
    trades_columns = trades_report[0][2:]
    trades_columns.pop(13)
    trades_columns.pop(12)
    trades_columns.pop(11)
    trades_columns.pop(9)
    trades_columns.pop(8)
    trades_data = trades_report[1:]
    trades_data_filtered = {k: [] for k in trades_columns}
    for row in iter(trades_data):
        if row[1] != 'Data' or row[2] not in ['Trade', 'ClosedLot']:
            continue

        # row[2] - type of order
        # row[3] - type of asset
        # row[4] - currency
        # row[5] - ticker
        # row[6] - date
        # row[7] - exchange
        # row[8] - quantity
        # row[9] - trade price
        # row[12] - fees
        # row[16] - code
        trades_data_filtered[trades_columns[0]].append(row[2])
        trades_data_filtered[trades_columns[1]].append(row[3])
        trades_data_filtered[trades_columns[2]].append(row[4])
        trades_data_filtered[trades_columns[3]].append(row[5])
        trades_data_filtered[trades_columns[4]].append(datetime.fromisoformat(row[6].replace(',', '')))
        trades_data_filtered[trades_columns[5]].append(row[7])
        trades_data_filtered[trades_columns[6]].append(row[8].replace(',', ''))
        trades_data_filtered[trades_columns[7]].append(row[9])
        trades_data_filtered[trades_columns[8]].append(row[12] if row[12] != '' else '0')
        trades_data_filtered[trades_columns[9]].append(row[16])

    df_trades = pl.DataFrame(trades_data_filtered).with_columns([
        pl.col('DataDiscriminator').cast(pl.Categorical),
        pl.col('Asset Category').cast(pl.Categorical),
        pl.col('Currency').cast(pl.Categorical),
        pl.col('Symbol').cast(pl.Categorical),
        pl.col('Exchange').cast(pl.Categorical),
        pl.col('Quantity').cast(pl.Float64),
        pl.col('T. Price').cast(pl.Float64),
        pl.col('Comm/Fee').cast(pl.Float64),
    ])
    # TODO split description into separate columns
    df_trades = df_trades.sort(by=df_trades.columns).unique()

    return df_trades
