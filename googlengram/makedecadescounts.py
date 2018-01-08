import argparse
import numpy as np

from multiprocessing import Process, Queue
import collections
from Queue import Empty

from ioutils import mkdir, write_pickle, load_pickle

def worker(proc_num, queue, out_dir, in_dir):
    while True:
        try:
            decade = queue.get(block=False)
        except Empty:
            break

        print "Processing decade", decade
        for year in range(10):
            year_counts = load_pickle(in_dir + str(decade + year) + "-counts.pkl")


            if year == 0:
                merged_year_counts = year_counts
            for word, count in year_counts.iteritems():
                if not word in merged_year_counts:
                    merged_year_counts[word] = 0
                merged_year_counts[word] += year_counts[word]

        write_pickle(merged_year_counts, out_dir + str(decade) + "-counts.pkl")

def run_parallel(num_procs, out_dir, in_dir, decades):
    queue = Queue()
    for decade in decades:
        queue.put(decade)
    procs = [Process(target=worker, args=[i, queue, out_dir, in_dir]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Merge counts for 1gram data.")
    parser.add_argument("base_dir", help="base directoty. /counts should be a subdir")
    parser.add_argument("num_procs", type=int, help="number of processes to spawn")
    parser.add_argument("--start-year", type=int, help="start year (inclusive)")
    parser.add_argument("--end-year", type=int, help="end year (inclusive)")
    args = parser.parse_args()
    decades = range(args.start_year, args.end_year+1, 10)
    decades.reverse()
    out_dir = args.base_dir + "/decades/counts/"
    mkdir(out_dir)
    run_parallel(args.num_procs, out_dir,  args.base_dir + "/counts/", decades)