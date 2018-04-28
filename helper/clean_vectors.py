from multiprocessing import Queue, Process

from argparse import ArgumentParser
from nltk.corpus import stopwords

import ioutils

VOCAB_FILE = "{year:d}-vocab.pkl"


def worker(proc_num, queue, out_dir, train_dir, vec_suffix, lang):
    while True:
        if queue.empty():
            break
        year = queue.get()

        print proc_num, "Cleaning vectors for year", year
        cleaned_vectors = clean_vectors(year, train_dir, vec_suffix, lang)
        print proc_num, "Write merged vectors for year", year
        write_vectors(year, out_dir, cleaned_vectors, vec_suffix)

    print proc_num, "Finished"


def clean_vectors(year, train_dir, vec_suffix, lang):
    stop_set = set(stopwords.words(lang))
    print(stop_set)
    cleaned_vectors = []
    vocab = ioutils.load_pickle(train_dir + VOCAB_FILE.format(year=year))
    print_vectors = load_file_lines(train_dir + str(year) + "-" + vec_suffix)

    for i, w in enumerate(vocab):
        if is_zero_vectors(print_vectors[i + 1].split()[1:]) or w in stop_set or not w.isalpha():
            continue
        cleaned_vectors.append(print_vectors[i + 1])
    return cleaned_vectors


def load_file_lines(queries_file):
    with open(queries_file) as f:
        lines = f.read().splitlines()
    return lines


def is_zero_vectors(vector):
    for v in vector:
        if float(v) > 1e-10:
            return False
    return True


def write_vectors(year, out_dir, cleaned_vectors, vec_suffix):
    vocab_size = len(cleaned_vectors)
    dim = len(cleaned_vectors[0].split()) - 1

    with open(out_dir + str(year) + vec_suffix, "w") as fwp:
        print >> fwp, vocab_size, dim
        for line in cleaned_vectors:
            print >> fwp, line.strip()


def run_parallel(workers, years, out_dir, train_dir, vec_suffix, lang):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, train_dir, vec_suffix, lang]) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == "__main__":
    parser = ArgumentParser("Clean vectors from stop words")
    parser.add_argument("train_dir", help="full trained vectors directory")
    parser.add_argument("out_dir", help="merged vectors out directory")
    parser.add_argument("vec_suffix", help="suffix used to name merged vectors")
    parser.add_argument("--lang", help="Language", default="english")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=8)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, years, args.out_dir + "/", args.train_dir + "/", args.vec_suffix, args.lang)
