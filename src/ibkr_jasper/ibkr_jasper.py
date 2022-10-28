import polars as pl
import csv
from pathlib import Path
from datetime import date

reports_path = Path('../../data')
report_files = [x for x in reports_path.glob('**/*') if x.is_file()]
report_reader = csv.reader(open(report_files[0]), delimiter=',')
report_list = list(report_reader)

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

print(df_io)
