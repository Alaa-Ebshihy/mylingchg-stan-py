import argparse
import os
import numpy as np

from multiprocessing import Process, Queue
from Queue import Empty

import ioutils
from representations.explicit import Explicit
from statutils.fastfreqdist import CachedFreqDist
SAMPLE_MAX = 1e9


def worker(proc_num, queue, out_dir, in_dir, count_dir, vocab_dir, sample=1e-5):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break
        print proc_num, "Getting counts and matrix year", year
        embed = Explicit.load(in_dir + str(year) + ".bin", normalize=False)
        freq = CachedFreqDist(ioutils.load_pickle(count_dir + str(year) + "-counts.pkl"))
        use_words = ioutils.load_word_list(vocab_dir + str(year) + ".vocab")
        embed = embed.get_subembed(use_words, restrict_context=True)
        sample_corr = min(SAMPLE_MAX / freq.N(), 1.0)
        print "Sample correction..", sample_corr
        embed.m = embed.m * sample_corr
        mat = embed.m.tocoo()
        print proc_num, "Outputing pairs for year", year
        with open(out_dir + str(year) + ".tmp.txt", "w") as fp:
            for i in xrange(len(mat.data)):
                if i % 10000 == 0:
                    print "Done ", i, "of", len(mat.data)
                word = embed.iw[mat.row[i]]
                context = embed.ic[mat.col[i]]
                if sample != 0:
                    prop_keep = min(np.sqrt(sample / freq.freq(word)), 1.0)
                    prop_keep *= min(np.sqrt(sample / freq.freq(context)), 1.0)
                else:
                    prop_keep = 1.0
                word = word.encode("utf-8")
                context = context.encode("utf-8")
                line = word + " " + context + "\n"
                for j in xrange(int(np.ceil(mat.data[i] * prop_keep))):
                    fp.write(line)
        print "shuf " + out_dir + str(year) + ".tmp.txt" " > " + out_dir + str(year) + ".txt"
        os.system("shuf " + out_dir + str(year) + ".tmp.txt" + " > " + out_dir + str(year) + ".txt")
        os.remove(out_dir + str(year) + ".tmp.txt")


def run_parallel(num_procs, out_dir, in_dir, count_dir, years, vocab_dir, sample):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, in_dir, count_dir, vocab_dir, sample]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Make pair corpus using existing vocabulary files.")
    parser.add_argument("out_dir")
    parser.add_argument("in_dir")
    parser.add_argument("count_dir")
    parser.add_argument("vocab_dir")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    parser.add_argument("--sample", type=float, default=1e-5)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.in_dir + "/", args.count_dir + "/", years, args.vocab_dir,
                 args.sample)
