import pandas as pd
import dateutil.rrule as rrule
from datetime import timedelta
from prettytable import PrettyTable
from src.ibkr_jasper.data_processing import get_port_for_date, get_portfolio_value, get_cur_month_deals_value, \
    get_cur_month_divs, get_period_return


def print_df(df_pl):
    with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 1000,
    ):
        df_pd = df_pl.to_pandas()
        print(df_pd)


def print_report(all_prices, tickers_total, buys_total, sells_total, divs_total, earliest_start_date):
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
        ret = get_period_return(cur_report_date, cur_end_date, tickers_total, buys_total, sells_total, divs_total,
                                all_prices)

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
