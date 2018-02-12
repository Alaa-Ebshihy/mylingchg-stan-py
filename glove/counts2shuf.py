import argparse
from scipy.sparse import dok_matrix, csr_matrix
import numpy as np
import random
from sys import getsizeof
import sys
import os


def worker(proc_num, queue, counts_dir, memory_size):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break

        print proc_num, "shuffling counts for year", year
        #shuffle round 1
        memory_size = memory_size * 1000**3
        counts = []
        counts_num_per_file = []
        tmp_id = 0
        with open(counts_dir + str(year) + "-pair_counts.tmp", 'r') as f:
            counts_num = 0
            for line in f:
                if counts_num % 1000 == 0:
                    sys.stdout.write("\r" + str(counts_num/1000**2) + "M counts processed.")
                counts_num += 1
                word, context, count = line.strip().split()
                counts.append((int(word), int(context), float(count)))
                if getsizeof(counts) + (getsizeof((int(0),int(0),float(0))) + getsizeof(int(0)) * 2 + getsizeof(float(0)) ) * len(counts) > memory_size:
                    random.shuffle(counts)
                    with open(counts_dir + str(year) + "-pair_counts.shuf" + str(tmp_id), 'w') as f:
                        for count in counts:
                            f.write(str(count[0]) + ' ' + str(count[1]) + ' ' + str(count[2]) + '\n')
                    counts_num_per_file.append(counts_num)
                    counts = []
                    tmp_id += 1

        random.shuffle(counts)
        with open(counts_dir + str(year) + "-pair_counts.shuf" + str(tmp_id), 'w') as f:
            for count in counts:
                f.write(str(count[0]) + ' ' + str(count[1]) + ' ' + str(count[2]) + '\n')
            counts = []
            tmp_id += 1
        if tmp_id == 1:
            counts_num_per_file.append(counts_num)

        print proc_num, "number of tmpfiles: ", tmp_id

        #shuffle round 2
        counts_num = 0
        output_file = open(counts_dir + str(year) + "-pair_counts.shuf", 'w')
        tmpfiles = []
        for i in xrange(tmp_id):
            tmpfiles.append(open(counts_dir + str(year) + "-pair_counts.shuf" + str(i), 'r'))

        tmp_num = counts_num_per_file[0] / tmp_id
        for i in xrange(tmp_id - 1):
            counts = []
            for f in tmpfiles:
                for j in xrange(tmp_num):
                    line = f.readline()
                    if len(line) > 0:
                        if counts_num % 1000 == 0:
                            print  str(counts_num/1000**2) + "M counts processed."
                        counts_num += 1
                        word, context, count = line.strip().split()
                        counts.append((int(word), int(context), float(count)))
            random.shuffle(counts)
            for count in counts:
                output_file.write(str(count[0]) + ' ' + str(count[1]) + ' ' + str(count[2]) + '\n')
        counts = []
        for f in tmpfiles:
            for line in f:
                if counts_num % 1000 == 0:
                    print str(counts_num/1000**2) + "M counts processed."
                counts_num += 1
                word, context, count = line.strip().split()
                counts.append((int(word), int(context), float(count)))
        random.shuffle(counts)
        for count in counts:
            output_file.write(str(count[0]) + ' ' + str(count[1]) + ' ' + str(count[2]) + '\n')

        for i in xrange(tmp_id):
            tmpfiles[i].close()
        for i in xrange(tmp_id):
            os.remove(counts_dir + str(year) + "-pair_counts.shuf" + str(i))
        output_file.close()
        print proc_num, "number of counts: ", counts_num


def run_parallel(num_procs, counts_dir, memory_size, years):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, vocab_dir, memory_size]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Computes various frequency statistics.")
    parser.add_argument("counts_dir", help="directory for count ngrams pairs")
    parser.add_argument("--memory-size", type=float, default=8.0)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.memory_size, years)
