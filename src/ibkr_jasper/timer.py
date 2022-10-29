from time import time_ns


class Timer:
    def __init__(self, message, do_print_log):
        self.message = message
        self.do_print_log = do_print_log

    def __enter__(self):
        if self.do_print_log:
            self.start = time_ns()
        return None  # could return anything, to be used like this: with Timer("Message") as value:

    def __exit__(self, type, value, traceback):
        if not self.do_print_log:
            return None

        elapsed_time = (time_ns() - self.start) / 1_000_000
        print(f'{self.message}: {elapsed_time:.6f}ms')
