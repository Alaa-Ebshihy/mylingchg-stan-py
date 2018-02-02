import numpy as np
from multiprocessing import Queue, Process
from argparse import ArgumentParser
import subprocess

from ioutils import load_pickle, write_pickle, mkdir
from glove_helper import text2numpy

def worker(proc_num, queue, dir, count_dir, min_count, checkpoints):
    while True:
        if queue.empty():
            break
        year = queue.get()
        freqs = load_pickle(count_dir + str(year) + "-counts.pkl")
        for n in checkpoints:
            out_dir =dir + '{:03d}'.format(n) + "/"
            mkdir(out_dir)
            subprocess.call(['mv', dir + str(year) + '-w.' + '{:03d}'.format(n), out_dir + str(year) + '-w'])
            print "Loading data..", year, "iterations", n
            text2numpy(out_dir, freqs, year)

if __name__ == "__main__":
    parser = ArgumentParser("Post-processes SGNS vectors to easier-to-use format. Removes infrequent words.")
    parser.add_argument("dir", help="Vectors directory.")
    parser.add_argument("count_dir", help="Directory with count data.")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=20)
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--year-inc", type=int, default=1)
    parser.add_argument("--min-count", type=int, default=500)
    parser.add_argument("--checkpoint-every", type=int, default=5)
    parser.add_argument("--iter", type=int, default=25)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    checkpoints = range(args.checkpoint_every, args.iter + 1, args.checkpoint_every)
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, args.dir, args.count_dir, args.min_count, checkpoints]) for i in range(args.workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()