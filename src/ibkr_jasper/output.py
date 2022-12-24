import pandas as pd
import dateutil.rrule as rrule
from datetime import date, timedelta
from prettytable import PrettyTable


def print_df(df_pl):
    with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 1000,
    ):
        df_pd = df_pl.to_pandas()
        print(df_pd)


def print_report(port):
    first_report_date = port.inception_date.replace(day=1)
    all_report_dates = list(rrule.rrule(rrule.MONTHLY, dtstart=first_report_date, until=date.today()))

    report_table = PrettyTable()
    report_table.align = 'r'
    report_table.field_names = [''] + port.tickers + ['start', 'deals', 'divs', 'end', 'return']
    for cur_report_date in all_report_dates:
        # start portfolio value
        port_start = port.get_port_for_date(cur_report_date)
        value_start = port.get_portfolio_value(port_start, cur_report_date)

        # deals and divs
        deals_value = port.get_cur_month_deals_value(cur_report_date)
        divs = port.get_cur_month_divs(cur_report_date)

        # end portfolio value
        cur_end_date = (cur_report_date + timedelta(days=32)).replace(day=1)
        port_end = port.get_port_for_date(cur_end_date)
        end_value = port.get_portfolio_value(port_end, cur_end_date)

        # return in percents
        ret = port.get_period_return(cur_report_date, cur_end_date)

        report_table.add_row([cur_report_date.date()] +
                             [f'{port_start[x]:.0f}' for x in port.tickers] +
                             [f'{value_start:.2f}'] +
                             [f'{deals_value:.2f}'] +
                             [f'{divs:.2f}'] +
                             [f'{end_value:.2f}'] +
                             [f'{100 * ret:.2f}%'])
        if cur_report_date.month == 12:
            report_table.add_row([''] * len(report_table.field_names))

    print(report_table)
