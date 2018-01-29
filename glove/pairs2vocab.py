import argparse
import os
import ioutils

from multiprocessing import Process, Queue
from Queue import Empty

from representations.matrix_serializer import save_count_vocabulary
import six
import sys

def worker(proc_num, queue, out_dir, in_dir):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break

        print proc_num, "pairs2vocab for year", year
        words_path = out_dir + str(year) + "-w.vocab"
        contexts_path = out_dir + str(year) + "-c.vocab"

        words = {} #center word vocabulary
        contexts = {} #context vocabulary


        print proc_num, "Processing pairs for year", year
        with open(in_dir + str(year) + ".txt") as f:
            pairs_num = 0
            for line in f:
                pairs_num += 1
                if pairs_num % 1000**2 == 0:
                    print str(int(pairs_num/1000**2)) + "M pairs processed."
                pair = line.strip().split()
                if pair[0] not in words :
                    words[pair[0]] = 1
                else:
                    words[pair[0]] += 1
                if pair[1] not in contexts :
                    contexts[pair[1]] = 1
                else:
                    contexts[pair[1]] += 1

        words = sorted(six.iteritems(words), key=lambda item: item[1], reverse=True)
        contexts = sorted(six.iteritems(contexts), key=lambda item: item[1], reverse=True)

        save_count_vocabulary(words_path, words)
        save_count_vocabulary(contexts_path, contexts)
        print ("words size: " + str(len(words)))
        print ("contexts size: " + str(len(contexts)))
        print ("number of pairs: " + str(pairs_num))
        print ("pairs2vocab finished")



def run_parallel(num_procs, out_dir, in_dir, years):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, in_dir]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Computes various frequency statistics.")
    parser.add_argument("out_dir", help="output directory for vocab ")
    parser.add_argument("in_dir", help="directory for ngrams pairs")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.in_dir + "/", years)

