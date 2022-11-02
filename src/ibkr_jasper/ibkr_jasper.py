from src.ibkr_jasper.input import *
from src.ibkr_jasper.output import *
from src.ibkr_jasper.timer import Timer
import numpy as np
import pandas as pd
import yfinance as yf
from yahoofinancials import YahooFinancials

with Timer("Read reports", True):
    report_list = load_raw_reports()

with Timer("Parse deposits & withdrawals", True):
    df_io = fetch_io(report_list)

with Timer("Parse trades", True):
    df_trades = fetch_trades(report_list)

all_tickers = np.transpose(df_trades.select('Symbol').unique().to_numpy()).tolist()[0]
start_date = df_trades.select('Date/Time').sort(by=['Date/Time'])[0].to_dicts()[0]['Date/Time']

with Timer("Loading of ETF prices", True):
    prices_dict = {}
    for ticker in all_tickers:
        if len(ticker) > 3:
            continue

        prices = yf.download(ticker,
                             start=start_date.date(),
                             end=date.today(),
                             progress=False)
        prices_dict[ticker] = pl.DataFrame(prices['Close'].reset_index())



print_df(df_trades)
