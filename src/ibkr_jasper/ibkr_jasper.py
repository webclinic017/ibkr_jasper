from src.ibkr_jasper.classes.portfolio import Portfolio
from src.ibkr_jasper.classes.total_portfolio import TotalPortfolio
from src.ibkr_jasper.output import print_report
from src.ibkr_jasper.timer import Timer

import polars as pl
pl.toggle_string_cache(True)


total_portfolio = TotalPortfolio().load()
port = Portfolio('LRP', total_portfolio).load()

with Timer('Output total portfolio table', True):
    print_report(port)
