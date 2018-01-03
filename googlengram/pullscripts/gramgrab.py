import re
import os
import subprocess
import collections
import argparse
from multiprocessing import Process, Queue
import random
import time

import ioutils
from googlengram import indexing
from representations import sparse_io_ref

VERSION = '20120701'
TYPE = '5gram'
POS = re.compile('.*_[A-Z]+\s.*')
LINE_SPLIT = 100000000
EXCLUDE_PATTERN = re.compile('.*_[A-Z]+[_,\s].*')

def update_count(ngram, target_ind, year, count, year_counters):
    item_id = ngram[target_ind]
    for i, context_id in enumerate(ngram):
        if i == target_ind:
            continue
        pair = (item_id, context_id)
        year_counters[year][pair] += count
    return year_counters

def main(proc_num, queue, out_dir, download_dir, context_size, is_zipped):
    print proc_num, "Start loop"
    while True:
        if queue.empty():
            break
        name = queue.get()

        if is_zipped:
            if not name.endswith((".gz")):
                continue
            print "Unzipping " + name + " ..."
            subprocess.call(['gunzip', '-f', download_dir + name, '-d'])
            name = name.split(".gz")[0]

        loc_dir = out_dir + "/" + name + "/"
        ioutils.mkdir(loc_dir)

        print proc_num, "Going through", name
        index = collections.OrderedDict()
        year_counters = collections.defaultdict(collections.Counter)
        time.sleep(120 * random.random())
        with open(download_dir + name) as f:
            for i, l in enumerate(f):
                split = l.strip().split('\t')
                if EXCLUDE_PATTERN.match(split[0]):
                    continue
                ngram = [indexing.word_to_id(word.split("_")[0], index) for word in split[0].split()]
                year = split[1]
                count = int(split[2])
                if context_size == 2:
                    year_counters = update_count(ngram, 2, year, count, year_counters)
                elif context_size == 4:
                    year_counters = update_count(ngram, 0, year, count, year_counters)
                    year_counters = update_count(ngram, 4, year, count, year_counters)
                else:
                    raise Exception("Unsupported context size")

        print proc_num, "Writing", name
        time.sleep(120 * random.random())
        sparse_io_ref.export_mats_from_dicts(year_counters, loc_dir)
        ioutils.write_pickle(index, loc_dir + "index.pkl")
        os.remove(download_dir + name)

def run_parallel(num_processes, root_dir, out_dir, context_size, is_zipped):
    queue = Queue()
    download_dir = root_dir + '/'
    out_dir = out_dir + '/c' + str(context_size) + '/raw/'
    ioutils.mkdir(out_dir)

    for name in os.listdir(download_dir):
        if name == ".DS_Store":
            continue
        queue.put(name)
    procs = [Process(target=main, args=[i, queue, out_dir, download_dir, context_size, is_zipped]) for i in range(num_processes)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parses 5gram data into co-occurrence matrices")
    parser.add_argument("root_dir", help="root directory where data lives")
    #parser.add_argument("source", help="source dataset to pull from (must be available on the N-Grams website")
    parser.add_argument("out_dir", help="directory where data will be stored")
    parser.add_argument("context_size", type=int, help="Size of context window. Currently only size 2 and 4 are supported.")
    parser.add_argument("num_procs", type=int, help="number of processes to spawn")
    parser.add_argument("--is_zipped", type=int, default=0 , help="If 0 then the files are unzipped. Otherwise, the input files are zipped")
    args = parser.parse_args()
    is_zipped = False if args.is_zipped == 0 else True

    run_parallel(args.num_procs, args.root_dir, args.out_dir, args.context_size, is_zipped)
