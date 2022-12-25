from src.ibkr_jasper.classes.portfolio import Portfolio
from src.ibkr_jasper.classes.total_portfolio import TotalPortfolio

import polars as pl
pl.toggle_string_cache(True)


total_portfolio = TotalPortfolio().load()
port = Portfolio('MaxReturn', total_portfolio).load()
port.print_report()
