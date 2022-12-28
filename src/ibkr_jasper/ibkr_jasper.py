import argparse
import polars as pl

from src.ibkr_jasper.cmd_functions import dispatcher

pl.toggle_string_cache(True)

parser = argparse.ArgumentParser(description='Welcome to IBKR Jasper - the best program for portfolio accounting')
parser.add_argument('command', type=str, help='type of command to execute')
parser.add_argument('args', type=str, nargs='*', help='parameters of command')
args = parser.parse_args()

try:
    function = dispatcher[args.command]
    function(*args.args)
except KeyError:
    print(f'command "{args.command}" not found')
