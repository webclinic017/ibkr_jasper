from src.ibkr_jasper.classes.portfolio import Portfolio
from src.ibkr_jasper.data_processing import get_all_etfs, get_portfolio_start_date, adjust_trades_by_splits, \
    get_tlh_trades
from src.ibkr_jasper.input import load_raw_reports, fetch_io, fetch_divs, fetch_trades
from src.ibkr_jasper.output import print_report
from src.ibkr_jasper.prices_loader import load_prices_and_splits, load_xrub_rates
from src.ibkr_jasper.timer import Timer

import polars as pl
pl.toggle_string_cache(True)


with Timer('Read reports', True):
    report_list = load_raw_reports()

with Timer('Parse deposits & withdrawals', True):
    io_total = fetch_io(report_list)

with Timer('Parse dividends', True):
    divs_total = fetch_divs(report_list)  # mb simpler to load them from yahoo finance

with Timer('Parse trades', True):
    trades_total = fetch_trades(report_list)
    tickers_total = get_all_etfs(trades_total)
    earliest_start_date = get_portfolio_start_date(trades_total)

with Timer('Loading of ETF prices', True):
    all_prices, all_splits = load_prices_and_splits(tickers_total, earliest_start_date)
    xrub_rates = load_xrub_rates(earliest_start_date)
    trades_total = adjust_trades_by_splits(trades_total, tickers_total, all_splits)

with Timer('Get trades for tax loss harvesting', True):
    tlh_trades = get_tlh_trades(trades_total, tickers_total, all_prices)

with Timer('Output total portfolio table', True):
    port = Portfolio('LRP')
    port.load_trades(trades_total)
    port.load_divs(divs_total)
    port.load_prices(all_prices)

    print_report(port)
