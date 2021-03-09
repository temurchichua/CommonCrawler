# This is a sample Python script.
import multiprocessing as mp
import argparse


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from data.indexes import month_indexes
from tools.cdx_handler import main_loop
from tools.timing import Timer
from tools.wet_handler import wet_line_to_text, notify


parser = argparse.ArgumentParser("CC handler processor")
parser.add_argument("--subs",
                    default=1,
                    help="Number of multiprocess.",
                    type=int)

args = parser.parse_args()
subs = args.subs
num_workers = mp.cpu_count()

if subs >= num_workers:
    print("Reduce the size of processes and try again")
    exit()
# Press the green button in the gutter to run the script.


if __name__ == '__main__':
    pool = mp.Pool(subs)
    message = f"Started new Work with {subs} processes"
    notify(message)
    main_loop(month_indexes[0], pool)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
