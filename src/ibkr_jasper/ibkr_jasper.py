from src.ibkr_jasper.input import *
from src.ibkr_jasper.output import *
from src.ibkr_jasper.timer import Timer

with Timer("Read reports", True):
    report_list = load_raw_reports()

with Timer("Parse deposits & withdrawals", True):
    df_io = fetch_io(report_list)

with Timer("Parse dividends", True):
    df_divs = fetch_divs(report_list)

print_df(df_divs)
