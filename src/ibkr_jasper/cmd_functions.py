from src.ibkr_jasper.classes.portfolio import Portfolio
from src.ibkr_jasper.classes.total_portfolio import TotalPortfolio


def status(portfolio_name):
    total_portfolio = TotalPortfolio().load()
    port = Portfolio(portfolio_name, total_portfolio).load()
    port.print_report()
    port.print_weights()


def tlh():
    total_portfolio = TotalPortfolio().load()
    total_portfolio.get_tlh_trades()
    total_portfolio.print_df(total_portfolio.tlh_trades)


dispatcher = {
    'status': status,
    'tlh': tlh,
}
