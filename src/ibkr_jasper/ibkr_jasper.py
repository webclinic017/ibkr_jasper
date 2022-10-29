from src.ibkr_jasper.input import *
from src.ibkr_jasper.output import *
from src.ibkr_jasper.timer import Timer

with Timer("Read reports", True):
    report_list = load_raw_reports()

with Timer("Parse deposits & withdrawals", True):
    df_io = fetch_io(report_list)

with Timer("Parse trades", True):
    df_trades = fetch_trades(report_list)

print_df(df_trades)
