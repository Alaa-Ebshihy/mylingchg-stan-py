from docopt import docopt
import argparse
from scipy.sparse import dok_matrix, csr_matrix
import numpy as np
import random
import struct
import sys


def worker(proc_num, queue, out_dir, count_dir):
    print "counts2bin"

    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break

        print proc_num, "Processing counts pairs for year", year

        bin_file = open(out_dir + str(year) + "-pair_counts.shuf.bin", 'wb')
        with open(count_dir + str(year) + "-pair_counts.shuf", 'r') as f:
            counts_num = 0
            for line in f:
                if counts_num % 1000 == 0:
                    sys.stdout.write("\r" + str(counts_num/1000**2) + "M tokens processed.")
                counts_num += 1
                word, context, count = line.strip().split()
                b = struct.pack('iid', int(word), int(context), float(count))
                bin_file.write(b)
        print proc_num, "number of counts: " + str(counts_num)
        bin_file.close()

    print proc_num, "Finished"

def run_parallel(num_procs, out_dir, count_dir, years):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, count_dir]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Computes various frequency statistics.")
    parser.add_argument("out_dir", help="output directory for bin count ngrams pairs")
    parser.add_argument("count_dir", help="directory contains count ngrams pairs -pair_counts.shuf")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.vocab_dir + "/", args.coo_dir + "/", years)
