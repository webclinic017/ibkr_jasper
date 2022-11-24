from src.ibkr_jasper.data_processing import get_etf_buys, get_etf_sells, get_all_etfs, get_portfolio_start_date, \
    get_port_for_date, get_portfolio_value, get_cur_month_deals_value, get_cur_month_divs, get_period_return
from src.ibkr_jasper.input import load_raw_reports, fetch_io, fetch_divs, fetch_trades
from src.ibkr_jasper.prices_loader import load_etf_prices
from src.ibkr_jasper.timer import Timer
from datetime import timedelta
from prettytable import PrettyTable
import dateutil.rrule as rrule

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
    first_report_date = earliest_start_date.replace(day=1)
    all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=earliest_start_date.today()))

    report_table = PrettyTable()
    report_table.align = 'r'
    report_table.field_names = [''] + tickers_total + ['start', 'deals', 'divs', 'end', 'return']
    for cur_report_date in all_report_dates:
        # start portfolio value
        port_start = get_port_for_date(tickers_total, cur_report_date, buys_total, sells_total)
        value_start = get_portfolio_value(port_start, all_prices, cur_report_date)

        # deals and divs
        deals_value = get_cur_month_deals_value(cur_report_date, buys_total, sells_total)
        divs = get_cur_month_divs(cur_report_date, divs_total)

        # end portfolio value
        cur_end_date = (cur_report_date + timedelta(days=32)).replace(day=1)
        port_end = get_port_for_date(tickers_total, cur_end_date, buys_total, sells_total)
        end_value = get_portfolio_value(port_end, all_prices, cur_end_date)

        # return in percents
        ret = get_period_return(cur_report_date, cur_end_date, tickers_total, buys_total, sells_total, divs_total, all_prices)

        report_table.add_row([cur_report_date.date()] +
                             [f'{port_start[x]:.0f}' for x in tickers_total] +
                             [f'{value_start:.2f}'] +
                             [f'{deals_value:.2f}'] +
                             [f'{divs:.2f}'] +
                             [f'{end_value:.2f}'] +
                             [f'{100 * ret:.2f}%'])
        if cur_report_date.month == 12:
            report_table.add_row([''] * len(report_table.field_names))

    print(report_table)
