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
    df_io = fetch_io(report_list)

with Timer('Parse dividends', True):
    df_divs = fetch_divs(report_list)

with Timer('Parse trades', True):
    df_trades = fetch_trades(report_list)
    df_etf_buys = get_etf_buys(df_trades)
    df_etf_sells = get_etf_sells(df_trades)
    all_etfs = get_all_etfs(df_trades)
    start_date = get_portfolio_start_date(df_trades)

with Timer('Loading of ETF prices', True):
    prices_df = load_etf_prices(all_etfs, start_date)

with Timer('Output total portfolio table', True):
    first_report_date = start_date.replace(day=1)
    all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=start_date.today()))

    report_table = PrettyTable()
    report_table.align = 'r'
    report_table.field_names = [''] + all_etfs + ['start', 'deals', 'divs', 'end', 'return']
    for start_date in all_report_dates:
        # start portfolio value
        port_start = get_port_for_date(all_etfs, start_date, df_etf_buys, df_etf_sells)
        start_value = get_portfolio_value(port_start, prices_df, start_date)

        # deals and divs
        deals_value = get_cur_month_deals_value(start_date, df_etf_buys, df_etf_sells)
        divs = get_cur_month_divs(start_date, df_divs)

        # end portfolio value
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        port_end = get_port_for_date(all_etfs, end_date, df_etf_buys, df_etf_sells)
        end_value = get_portfolio_value(port_end, prices_df, end_date)

        # return in percents
        ret = get_period_return(start_date, end_date, all_etfs, df_etf_buys, df_etf_sells, df_divs, prices_df)

        report_table.add_row([start_date.date()] +
                             [f'{port_start[x]:.0f}' for x in all_etfs] +
                             [f'{start_value:.2f}'] +
                             [f'{deals_value:.2f}'] +
                             [f'{divs:.2f}'] +
                             [f'{end_value:.2f}'] +
                             [f'{100 * ret:.2f}%'])
        if start_date.month == 12:
            report_table.add_row([''] * len(report_table.field_names))

    print(report_table)
