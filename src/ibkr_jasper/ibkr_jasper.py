from src.ibkr_jasper.input import *
from src.ibkr_jasper.output import *
from src.ibkr_jasper.timer import Timer
import numpy as np
import pandas as pd
import yfinance as yf
from yahoofinancials import YahooFinancials
from datetime import timedelta
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


with Timer('Group trades', True):
    all_tickers = np.transpose(df_trades.select('Symbol').unique().to_numpy()).tolist()[0]
    all_etfs = [x for x in all_tickers if len(x) <= 4]
    start_date = df_trades.select('Date/Time').sort(by=['Date/Time'])[0].to_dicts()[0]['Date/Time']

with Timer('Loading of ETF prices', True):
    prices_dict = {}
    for ticker in all_tickers:
        if len(ticker) > 4:
            continue

        prices = yf.download(ticker,
                             start=start_date.date(),
                             end=date.today(),
                             progress=False)
        prices_dict[ticker] = pl.DataFrame(prices['Close'].reset_index()).rename({'Close': ticker})

with Timer('Convert prices dict to 1 polars table', True):
    prices_df = None
    for ticker, prices in prices_dict.items():
        if prices_df is None:
            prices_df = prices
        else:
            prices_df = prices_df.join(prices, on='Date', how='outer')

with Timer('Output portfolio table', True):
    first_report_date = (start_date.replace(day=1) + timedelta(days=32)).replace(day=1)
    all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=date.today()))

    report_table = PrettyTable()
    report_table.field_names = all_etfs
    for date in all_report_dates:


        report_table.add_row(["Adelaide", 1295, 1158259, 600.5])
