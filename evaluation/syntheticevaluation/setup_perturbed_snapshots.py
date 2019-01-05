from multiprocessing import Queue, Process

from argparse import ArgumentParser
import ioutils

import copy
from scipy.stats import bernoulli


def worker(proc_num, queue, corpus, donor_list, donor_occuarances_map, receptor_list, word_freq, preplacement, out_dir, out_suffix):
    while True:
        if queue.empty():
            break
        year = queue.get()

        print proc_num, "Creating snapshot for year", year
        new_corpus = copy.copy(corpus)
        n = len(donor_list)
        for i in range(n):
            d = donor_list[i]
            perturb_word_in_corpus(new_corpus, d, donor_occuarances_map[d], receptor_list[i],
                                   word_freq[donor_list[i]], preplacement)
        print proc_num, "Write snapshot for year", year
        write_snapshot(year, out_dir, out_suffix, new_corpus)

    print proc_num, "Finished"


def perturb_word_in_corpus(corpus, donor, donor_occuarances, receptor, donor_count, preplacement):
    data_bern = bernoulli.rvs(size=donor_count, p=preplacement)
    # donor_idx = 0
    for idx, p in enumerate(data_bern):
        if p == 1:
            corpus[donor_occuarances[idx]] = receptor
    # for idx, w in enumerate(corpus):
    #     if w == donor:
    #         if data_bern[donor_idx] == 1:
    #             corpus[idx] = receptor
    #         donor_idx += 1


def write_snapshot(year, out_dir, out_suffix, corpus):
    with open(out_dir + str(year) + out_suffix, 'w') as outfile:
        outfile.writelines(' '.join(corpus) + "\n")


def run_parallel(workers, years, corpus, donor_list, donor_occuarances_map, receptor_list, word_freq, preplacement, out_dir, out_suffix):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, corpus, donor_list, donor_occuarances_map, receptor_list, word_freq, preplacement, out_dir, out_suffix]) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


def read_corpus_to_list(corpus_file_path):
    with open(corpus_file_path) as f:
        return [word for line in f for word in line.split()]


def read_word_freq(word_freq_path):
    word_count_map = {}
    with open(word_freq_path) as f:
        for line in f:
            (key, val) = line.split()
            word_count_map[key] = int(val)
    return word_count_map

def get_donor_occurances(corpus, donor_list):
    print "Setting donor occurances map"
    donor_set = set(donor_list)
    donor_occuarances_map = {}

    for idx, w in enumerate(corpus):
        if w in donor_set:
            if w not in donor_occuarances_map:
                donor_occuarances_map[w] = []
            donor_occuarances_map[w].append(idx)
    return donor_occuarances_map


if __name__ == "__main__":
    parser = ArgumentParser("Setup perturbed snapshots from corpus")
    parser.add_argument("corpus_file_path", help="path to raw data corpus")
    parser.add_argument("donor_list_path", help="path to donor words")
    parser.add_argument("receptor_list_path", help="path to receptor words")
    parser.add_argument("word_freq_path", help="path to words freq of corpus")
    parser.add_argument("out_dir", help="out directory")
    parser.add_argument("out_suffix", help="suffix used to name")
    parser.add_argument("--preplacement", type=float, help="success probability", default=0.1)
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=8)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1900)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)

    corpus = read_corpus_to_list(args.corpus_file_path)
    donor_list = ioutils.load_word_list(args.donor_list_path)
    donor_occuarances_map = get_donor_occurances(corpus, donor_list)

    run_parallel(args.workers, years, corpus, donor_list, donor_occuarances_map,
                 ioutils.load_word_list(args.receptor_list_path), read_word_freq(args.word_freq_path),
                 args.preplacement, args.out_dir + "/", args.out_suffix)

