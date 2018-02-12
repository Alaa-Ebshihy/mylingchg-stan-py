import argparse
import os
import numpy as np

from multiprocessing import Process, Queue
from Queue import Empty

import ioutils
from representations.explicit import Explicit
from statutils.fastfreqdist import CachedFreqDist
SAMPLE_MAX = 1e9

def worker(proc_num, queue, out_dir, in_dir, count_dir, valid_words, num_words, min_count, sample=1e-5):
    while True:
        try:
            year = queue.get(block=False)
        except Empty:
            break
        print proc_num, "Getting counts and matrix year", year
        embed = Explicit.load(in_dir + str(year) + ".bin", normalize=False)
        year_words = valid_words[year][:num_words]
        count_words = set(ioutils.words_above_count(count_dir, year, min_count))
        freq = CachedFreqDist(ioutils.load_pickle(count_dir + str(year) + "-counts.pkl"))
        use_words = list(count_words.intersection(year_words))
        embed = embed.get_subembed(use_words, restrict_context=True)
        sample_corr = min(SAMPLE_MAX / freq.N(), 1.0)
        print "Sample correction..", sample_corr
        embed.m = embed.m * sample_corr
        mat = embed.m.tocoo()
        vocab_dict = {k: v for v, k in enumerate(year_words)}
        print proc_num, "Outputing pairs for year", year
        with open(out_dir + str(year) + "-pair.txt", "w") as fp, open(out_dir + str(year) + "-pair_counts.tmp", "w") as fpc:
            for i in xrange(len(mat.data)):
                if i % 10000 == 0:
                    print "Done ", i, "of", len(mat.data)
                word_context_count = int(mat.data[i])
                if word_context_count <> 0:
                    word = embed.iw[mat.row[i]]
                    context = embed.ic[mat.col[i]]
                    if sample != 0:
                        prop_keep = min(np.sqrt(sample / freq.freq(word)), 1.0)
                        prop_keep *= min(np.sqrt(sample / freq.freq(context)), 1.0)
                    else:
                        prop_keep = 1.0
                    fpc.write(str(vocab_dict[word]) + " " + str(vocab_dict[context]) + " " + str(word_context_count) + "\n" )
                    word = word.encode("utf-8")
                    context = context.encode("utf-8")
                    fp.write(word + " " + context + " " + str(word_context_count) + "\n")


                #for j in xrange(int(mat.data[i] * prop_keep)):
                #fp.write(line)
        mat = mat.tocsr()
        print proc_num, "Outputing vocab for year", year
        with open(out_dir + str(year) + ".vocab", "w") as fp:
            for word in year_words:
                if not word in count_words or not word in embed.wi:
                    print >>fp, word.encode("utf-8"), 1
                else:
                    print >>fp, word.encode("utf-8"), int(mat[embed.wi[word], :].sum())

        print "shuf " + out_dir + str(year) + "-pair_counts.tmp" " > " + out_dir + str(year) + "-pair_counts.shuf"
        os.system("shuf " + out_dir + str(year) + "-pair_counts.tmp" + " > " + out_dir + str(year) + "-pair_counts.shuf")
        os.remove(out_dir + str(year) + "-pair_counts.tmp")

def run_parallel(num_procs, out_dir, in_dir, count_dir, years, words, num_words, min_count, sample):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, in_dir, count_dir, words, num_words, min_count, sample]) for i in range(num_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Computes various frequency statistics.")
    parser.add_argument("out_dir")
    parser.add_argument("in_dir")
    parser.add_argument("count_dir")
    parser.add_argument("word_file", help="file maps from year to word list (the output of freqperyear)")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--num-words", type=int, default=None)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=2000)
    parser.add_argument("--year-inc", type=int, help="end year (inclusive)", default=1)
    parser.add_argument("--min-count", type=int, default=100)
    parser.add_argument("--sample", type=float, default=1e-5)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    words = ioutils.load_year_words(args.word_file, years)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, args.out_dir + "/", args.in_dir + "/", args.count_dir + "/", years, words, args.num_words, args.min_count, args.sample)