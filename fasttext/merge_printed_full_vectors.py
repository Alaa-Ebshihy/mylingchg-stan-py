from multiprocessing import Queue, Process

from argparse import ArgumentParser

import ioutils

"""
merge the word vectors printed and the vectors used in training to use them in time series analysis
"""
TRAINED_VEC_FILE = "{year:d}-w.vec"
VOCAB_FILE = "{year:d}-vocab.pkl"


def worker(proc_num, queue, out_dir, train_dir, print_dir, queries_list, out_vec_suffix):
    while True:
        if queue.empty():
            break
        year = queue.get()

        print proc_num, "Merging vectors for year", year
        extra_vectors = get_extra_vectors(year, train_dir, print_dir, queries_list)
        print proc_num, "Write merged vectors for year", year
        write_merged_vectors(year, train_dir, out_dir, extra_vectors, out_vec_suffix)

    print proc_num, "Finished"


def get_extra_vectors(year, train_dir, print_dir, queries_list):
    extra_vectors = []
    vocab = ioutils.load_pickle(train_dir + VOCAB_FILE.format(year=year))
    print_vectors = load_file_lines(print_dir + TRAINED_VEC_FILE.format(year=year))

    for i, w in enumerate(queries_list):
        if w.decode("utf-8") not in vocab:
            extra_vectors.append(print_vectors[i + 1])
    return extra_vectors


def write_merged_vectors(year, train_dir, out_dir, extra_vectors, out_vec_suffix):
    with open(train_dir + TRAINED_VEC_FILE.format(year=year)) as fp:
        info = fp.readline().split()
        vocab_size = int(info[0]) + len(extra_vectors)
        dim = int(info[1])

        with open(out_dir + str(year) + out_vec_suffix, "w") as fwp:
            print >> fwp, vocab_size, dim
            for line in fp:
                print >> fwp, line.strip()
                #fwp.write(line.strip())
            for line in extra_vectors:
                print >> fwp, line.strip()
                #fwp.write(line.strip())


def run_parallel(workers, years, out_dir, train_dir, print_dir, queries_list, out_vec_suffix):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, out_dir, train_dir, print_dir, queries_list, out_vec_suffix]) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


def load_file_lines(queries_file):
    with open(queries_file) as f:
        lines = f.read().splitlines()
    return lines


if __name__ == "__main__":
    parser = ArgumentParser("Setup queries file given range of years and word-list contains target words")
    parser.add_argument("train_dir", help="full trained vectors directory")
    parser.add_argument("print_dir", help="printed vectors directory")
    parser.add_argument("out_dir", help="merged vectors out directory")
    parser.add_argument("out_vec_suffix", help="suffix used to name merged vectors")
    parser.add_argument("queries_file", help="path of vocab files used to generate printed vectors")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=8)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    run_parallel(args.workers, years, args.out_dir + "/", args.train_dir + "/", args.print_dir + "/",
                 load_file_lines(args.queries_file), args.out_vec_suffix)
