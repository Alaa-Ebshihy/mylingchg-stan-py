import argparse
import os
import numpy as np

from multiprocessing import Process, Queue
from Queue import Empty

import ioutils
from representations.explicit import Explicit

def worker(proc_num, queue, out_dir, vocab_dir, coo_dir):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break

        print proc_num, "Loading vocabulary for year"
        vocab = load_vocabulary(year, vocab_dir)

        print proc_num, "Getting counts and matrix year", year
        embed = Explicit.load(coo_dir + str(year) + ".bin", normalize=False)
        mat = embed.m.tocoo().tocsr()

        print proc_num, "Outputing count pairs for year", year

        vocab_len = len(vocab)
        with open(out_dir + str(year) + "-pair_counts", "w") as fp:
            for w in range vocab_len:
                for c in range vocab_len:
	        	    if int(mat[embed.wi[vocab[w]], embed.ci[vocab[c]]]) <> 0:
                        print >>fp, w, c, mat[embed.wi[vocab[w]], embed.ci[vocab[c]]]

def load_vocabulary(year, vocab_dir)
    vocab = []
    with open(vocab_dir + str(year) + ".vocab") as f:
        vocab = f.read().splitlines()
    return vocab

def run_parallel(num_procs, out_dir, vocab_dir, coo_dir, years):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, vocab_dir, coo_dir, words]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Computes various frequency statistics.")
    parser.add_argument("out_dir", help="output directory for count ngrams pairs")
    parser.add_argument("vocab_dir", help="directory contains .vocab files to be used in training")
    parser.add_argument("coo_dir", help="directory contains coocurrence data \{year\}.bin")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.vocab_dir + "/", args.coo_dir + "/", years)
