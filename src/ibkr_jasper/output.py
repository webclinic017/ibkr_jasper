import pandas as pd


def print_df(df_pl):
    with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 1000,
    ):
        df_pd = df_pl.to_pandas()
        print(df_pd)
