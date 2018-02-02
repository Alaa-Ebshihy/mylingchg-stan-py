import argparse
import os
import numpy as np

from multiprocessing import Process, Queue
from Queue import Empty

import ioutils

from collections import Counter
from math import sqrt
from sys import getsizeof
import cPickle as pickle

def worker(proc_num, queue, out_dir, vocab_dir, memory_size):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break

        print proc_num, "Loading vocabulary for year", year
        wi, iw =load_vocabulary(vocab_dir + str(year) + ".vocab")
        ci, ic = load_vocabulary(vocab_dir + str(year) + ".vocab")
        memory_size = memory_size * 1000**3
        D = {} #store co-occurrence matrix in dictionary
        tmpfile_num = 0
        memory_size_used = 0

        print proc_num, "Outputing count pairs for year", year

        with open(vocab_dir + str(year) + ".txt", "r") as f:
            pairs_num = 0
            print str(pairs_num/1000**2) + "M pairs processed."
            for line in f:
                pairs_num += 1
                if pairs_num % 1000**2 == 0:
                    print str(pairs_num/1000**2) + "M pairs processed."
                if getsizeof(D) + memory_size_used > memory_size * 0.8: #write dictionary to disk when memory is insufficient
                    with open(out_dir + str(year) + "-pair_counts-tmp-" + str(tmpfile_num), 'wb') as f:
                        tmp_sorted = sorted(D.keys())
                        for i in tmp_sorted:
                            pickle.dump((i, D[i]), f, True)
                        D.clear()
                        memory_size_used = 0
                        tmpfile_num += 1
                pair = line.strip().split()
                word_index = wi[pair[0]]
                context_index = ci[pair[1]]
                if word_index in D:
                    tmp_size = getsizeof(D[word_index])
                    D[word_index].update({context_index: 1})
                    memory_size_used += getsizeof(D[word_index]) - tmp_size #estimate the size of memory used
                else:
                    D[word_index] = Counter({context_index: 1})
                    memory_size_used += getsizeof(D[word_index])
        with open(out_dir + str(year) + "-pair_counts-tmp-" + str(tmpfile_num), 'wb') as f:
            tmp_sorted = sorted(D.keys())
            for i in tmp_sorted:
                pickle.dump((i, D[i]), f, True)
            D.clear()
            tmpfile_num += 1


        #merge tmpfiles to co-occurrence matrix
        tmpfiles = []
        top_buffer = [] #store top elements of tmpfiles
        counts_num = 0
        counts_file = open(out_dir + str(year) + "-pair_counts", 'w')
        for i in xrange(tmpfile_num):
            tmpfiles.append(open(out_dir + str(year) + "-pair_counts-tmp-" + str(i), 'rb'))
            top_buffer.append(pickle.load(tmpfiles[i]))
        old = top_buffer[0]
        top_buffer[0] = pickle.load(tmpfiles[0])
        print str(counts_num/1000**2) + "M counts processed."
        while True:
            arg_min = np.argmin(np.asarray([c[0] for c in top_buffer])) #find the element with smallest key (center word)
            if top_buffer[arg_min][0] == old[0]: #merge values when keys are the same
                old[1].update(top_buffer[arg_min][1])
            else:
                tmp_sorted = sorted(old[1].keys()) #write the old element when keys are different (which means all pairs whose center words are [old.key] are aggregated)
                for w in tmp_sorted:
                    counts_num += 1
                    if counts_num % 1000**2 == 0:
                        print str(counts_num/1000**2) + "M counts processed."
                    counts_file.write(str(old[0]) + " " + str(w) + " " + str(old[1][w]) + "\n")
                old = top_buffer[arg_min]
            try:
                top_buffer[arg_min] = pickle.load(tmpfiles[arg_min])
            except EOFError: #when elements in file are exhausted
                top_buffer[arg_min] = (np.inf, Counter())
                tmpfile_num -= 1
            if tmpfile_num == 0:
                tmp_sorted = sorted(old[1].keys())
                for w in tmp_sorted:
                    counts_num += 1
                    counts_file.write(str(old[0]) + " " + str(w) + " " + str(old[1][w]) + "\n")
                break
        counts_file.close()
        print "number of counts: ", counts_num
        for i in xrange(len(top_buffer)): #remove tmpfiles
            os.remove(out_dir + str(year) + "-pair_counts-tmp-" + str(i))

        print proc_num, "Finished counts for year", year

        print "shuf " + out_dir + str(year) + "-pair_counts" + " > " + out_dir + str(year) + "-pair_counts.shuf"
        os.system("shuf " + out_dir + str(year) + "-pair_counts" + " > " + out_dir + str(year) + "-pair_counts.shuf")
        os.remove(out_dir + str(year) + "-pair_counts")

    print proc_num, "Finished"


def load_vocabulary(vocab_path):
    with open(vocab_path) as f:
        vocab = [line.strip().split()[0] for line in f if len(line) > 0]
    return dict([(a, i) for i, a in enumerate(vocab)]), vocab

def run_parallel(num_procs, out_dir, vocab_dir, memory_size, years):
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
    parser.add_argument("out_dir", help="output directory for count ngrams pairs")
    parser.add_argument("vocab_dir", help="directory contains .vocab files to be used in training")
    #parser.add_argument("coo_dir", help="directory contains coocurrence data {year}.bin")
    parser.add_argument("--memory-size", type=float, default=8.0)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.vocab_dir + "/", args.memory_size, years)