import numpy as np
from multiprocessing import Queue, Process
from argparse import ArgumentParser

from ioutils import load_pickle, write_pickle
from glove_helper import text2numpy

def worker(proc_num, queue, dir, count_dir, min_count):
    while True:
        if queue.empty():
            break
        year = queue.get()
        print "Loading data..", year
#        time.sleep(120 * random.random())
        freqs = load_pickle(count_dir + str(year) + "-counts.pkl")
        text2numpy(dir, freqs, year)

if __name__ == "__main__":
    parser = ArgumentParser("Post-processes SGNS vectors to easier-to-use format. Removes infrequent words.")
    parser.add_argument("dir", help="Vectors directory.")
    parser.add_argument("count_dir", help="Directory with count data.")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=20)
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--year-inc", type=int, default=1)
    parser.add_argument("--min-count", type=int, default=500)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, args.dir, args.count_dir, args.min_count]) for i in range(args.workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()