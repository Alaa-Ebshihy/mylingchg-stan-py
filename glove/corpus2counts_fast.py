import argparse
import os
import numpy as np

from collections import Counter

from multiprocessing import Process, Queue
from Queue import Empty

import ioutils
from representations.explicit import Explicit

MAX_PAIR_COUNT = 15000000
def worker(proc_num, queue, out_dir, vocab_dir, coo_dir):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break

        print proc_num, "Loading vocabulary for year", year
        vocab = load_vocabulary(year, vocab_dir)

        print proc_num, "Loading pairs for year", year
        #context_list = load_context(year, vocab_dir)

        print proc_num, "Getting counts and matrix year", year
        embed = Explicit.load(coo_dir + str(year) + ".bin", normalize=False)
        mat = embed.m.tocoo().tocsr()

        print proc_num, "Outputing count pairs for year", year

        coo_dict = {}
        count_tmp_files = 0
        with open(vocab_dir + str(year) + ".txt", "r") as fcp:
            count_tmp_pairs = 0
            for pair in fcp:
                count_tmp_pairs += 1
                word = pair.split()[0]
                context = pair.split()[1]
                if (word in embed.wi) and (context in embed.ci) and (int(mat[embed.wi[word], embed.ci[context]]) <> 0):
                    if vocab[word] in coo_dict:
                        coo_dict[vocab[word]].update({vocab[context]: mat[embed.wi[word], embed.ci[context]]})
                    else:
                        coo_dict[vocab[word]] = Counter({vocab[context]: mat[embed.wi[word], embed.ci[context]]})
                if count_tmp_pairs == MAX_PAIR_COUNT:
                    print proc_num, "Writing temp file", count_tmp_files, "for year", year
                    ioutils.write_pickle(coo_dict, out_dir + str(year) + "-pair_counts-tmp-" + str(count_tmp_files) + ".pkl")
                    count_tmp_files += 1;
                    count_tmp_pairs = 0;
                    coo_dict.clear()
            ioutils.write_pickle(coo_dict, out_dir + str(year) + "-pair_counts-tmp-" + str(count_tmp_files) + ".pkl")
            count_tmp_files += 1;
            count_tmp_pairs = 0;
            coo_dict.clear()

        print proc_num, "Merging temp count for year", year
        merge_tmp_count(out_dir, count_tmp_files, year)

def merge_tmp_count(out_dir, count_tmp_files, year):
    with open(out_dir + str(year) + "-pair_counts.tmp", "w") as fp:
        for i in range (count_tmp_files):
            coo_dict = ioutils.load_pickle(out_dir + str(year) + "-pair_counts-tmp-" + str(i) + ".pkl")
            for word_idx in coo_dict:
                context_keys = coo_dict[word_idx].keys()
                for context_idx in context_keys:
                    print >>fp, word_idx, context_idx, coo_dict[word_idx][context_idx]
            os.remove(out_dir + str(year) + "-pair_counts-tmp-" + str(i) + ".pkl")
            coo_dict.clear()
        print "shuf " + out_dir + str(year) + "-pair_counts.tmp" + " > " + out_dir + str(year) + "-pair_counts.shuf"
        os.system("shuf " + out_dir + str(year) + "-pair_counts.tmp" + " > " + out_dir + str(year) + "-pair_counts.shuf")
        os.remove(out_dir + str(year) + "-pair_counts.tmp" + str(i) + ".pkl")

def load_vocabulary(year, vocab_dir):
    vocab = {}
    with open(vocab_dir + str(year) + ".vocab") as f:
        lines = f.read().splitlines()
        vocab_list = [ line.split()[0] for line in lines]
        vocab = {k: v for v, k in enumerate(vocab_list)}
    return vocab

def load_context(year, vocab_dir):
    context_list = []
    with open(vocab_dir + str(year) + ".txt") as f:
        context_list =  f.read().splitlines()
    return context_list

def run_parallel(num_procs, out_dir, vocab_dir, coo_dir, years):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, vocab_dir, coo_dir]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Computes various frequency statistics.")
    parser.add_argument("out_dir", help="output directory for count ngrams pairs")
    parser.add_argument("vocab_dir", help="directory contains .vocab files to be used in training")
    parser.add_argument("coo_dir", help="directory contains coocurrence data {year}.bin")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.vocab_dir + "/", args.coo_dir + "/", years)