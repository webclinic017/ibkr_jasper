from src.ibkr_jasper.data_processing import get_etf_buys, get_etf_sells, get_all_etfs, get_portfolio_start_date
from src.ibkr_jasper.input import load_raw_reports, fetch_io, fetch_divs, fetch_trades
from src.ibkr_jasper.prices_loader import load_etf_prices
from src.ibkr_jasper.timer import Timer
from datetime import date, timedelta
from prettytable import PrettyTable
import dateutil.rrule as rrule

with Timer('Read reports', True):
    report_list = load_raw_reports()

with Timer('Parse deposits & withdrawals', True):
    df_io = fetch_io(report_list)

with Timer('Parse dividends', True):
    df_divs = fetch_divs(report_list)

with Timer('Parse trades', True):
    df_trades = fetch_trades(report_list)
    df_etf_buys = get_etf_buys()
    df_etf_sells = get_etf_sells()
    all_etfs = get_all_etfs(df_trades)
    start_date = get_portfolio_start_date(df_trades)

with Timer('Loading of ETF prices', True):
    prices_df = load_etf_prices(all_etfs, start_date)

with Timer('Output portfolio table', True):
    first_report_date = (start_date.replace(day=1) + timedelta(days=32)).replace(day=1)
    all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=date.today()))

    report_table = PrettyTable()
    report_table.field_names = all_etfs
    for date in all_report_dates:
        report_table.add_row(["Adelaide", 1295, 1158259, 600.5])
