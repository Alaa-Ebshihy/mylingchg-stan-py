from multiprocessing import Queue, Process
from argparse import ArgumentParser

import ioutils

"""
setup queries file to be used in printing word vectors for years
"""
SAVE_FILE = "{year:d}-query.txt"
FULL_RANGE_SAVE_FILE = "{year1:d}-{year2:d}-query.txt"

def worker(proc_num, queue, out_dir, target_lists):
    while True:
        if queue.empty():
            break
        year = queue.get()
        print proc_num, "Setting queries for year ..", year
        with open(out_dir + SAVE_FILE.format(year=year), "w") as fp:
            for word in target_lists[year]:
                print >>fp, word.encode("utf-8")

def run_parallel(workers, years, out_dir, target_lists):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, target_lists]) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    full_word_set = set([])
    for year_words in target_lists.itervalues():
        full_word_set = full_word_set.union(set(year_words))
    with open(out_dir + FULL_RANGE_SAVE_FILE.format(year1=years[0], year2=years[-1]), "w") as fp:
        for word in full_word_set:
            print >>fp, word.encode("utf-8")

if __name__ == "__main__":
    parser = ArgumentParser("Setup queries file given range of years and word-list contains target words")
    parser.add_argument("out_dir", help="output path")
    parser.add_argument("word_file", help="path to sorted word file (target word list)")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=20)
    parser.add_argument("--target-words", type=int, help="Number of words (of decreasing average frequency) to analyze", default=-1)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    target_lists, context_lists = ioutils.load_target_context_words(years, args.word_file, args.target_words, -1)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, years, args.out_dir + "/", target_lists)