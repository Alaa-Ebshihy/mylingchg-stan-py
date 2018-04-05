"""
Helper file to include functions to clean .vocab files from non-alphabtic characters
"""

import re
import ioutils

from multiprocessing import Queue, Process

from argparse import ArgumentParser


EXCLUDE_PATTERN = re.compile('[^A-Za-z]+')

VOCAB_FILE = "{year:d}-vocab.pkl"


def worker(proc_num, queue, out_dir, input_dir, out_suffix):
    while True:
        if queue.empty():
            break
        year = queue.get()

        print proc_num, "Cleaning vocab of year", year
        vocab_list = ioutils.load_pickle(input_dir + VOCAB_FILE.format(year=year))
        cleaned_vocab_list = remove_non_alph(vocab_list)
        ioutils.write_list(out_dir + str(year) + out_suffix, cleaned_vocab_list)


def remove_non_alph(vocab_list):
    cleaned_vocab_list = []
    for w in vocab_list:
        if EXCLUDE_PATTERN.match(w.encode("utf-8")):
            continue
        cleaned_vocab_list.append(w)
    return cleaned_vocab_list


def run_parallel(workers, years, out_dir, input_dir, out_suffix):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, input_dir, out_suffix]) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == "__main__":
    parser = ArgumentParser("Setup queries file given range of years and word-list contains target words")
    parser.add_argument("input_dir", help="Input directory of -vocab.pkl files")
    parser.add_argument("out_dir", help="Output directory of cleaned vocab")
    parser.add_argument("out_suffix", help="suffix used to name cleaned vocab with extension")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=8)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, years, args.out_dir + "/", args.input_dir + "/", args.out_suffix)