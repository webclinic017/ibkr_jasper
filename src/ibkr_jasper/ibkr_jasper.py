from src.ibkr_jasper.data_processing import get_etf_buys, get_etf_sells, get_all_etfs, get_portfolio_start_date
from src.ibkr_jasper.input import load_raw_reports, fetch_io, fetch_divs, fetch_trades
from src.ibkr_jasper.output import print_report
from src.ibkr_jasper.prices_loader import load_etf_prices
from src.ibkr_jasper.timer import Timer

with Timer('Read reports', True):
    report_list = load_raw_reports()

with Timer('Parse deposits & withdrawals', True):
    io_total = fetch_io(report_list)

with Timer('Parse dividends', True):
    divs_total = fetch_divs(report_list)

with Timer('Parse trades', True):
    trades_total = fetch_trades(report_list)
    buys_total = get_etf_buys(trades_total)
    sells_total = get_etf_sells(trades_total)
    tickers_total = get_all_etfs(trades_total)
    earliest_start_date = get_portfolio_start_date(trades_total)

with Timer('Loading of ETF prices', True):
    all_prices = load_etf_prices(tickers_total, earliest_start_date)

with Timer('Output total portfolio table', True):
    print_report(all_prices, tickers_total, buys_total, sells_total, divs_total, earliest_start_date)
