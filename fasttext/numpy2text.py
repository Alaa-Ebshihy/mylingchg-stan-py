import numpy as np
from multiprocessing import Queue, Process
from argparse import ArgumentParser

from ioutils import load_pickle


def worker(proc_num, queue, vec_path):
    while True:
        if queue.empty():
            break
        year = queue.get()
        print proc_num, "Loading data..", year
        numpy2text(vec_path, year, ".vec")


def numpy2text(vec_path, year, extension):
    vocab_list = load_pickle(vec_path + str(year) + "-vocab.pkl")
    w_mat = np.load(vec_path + str(year) + "-w.npy")
    vocab_size = len(vocab_list)
    dim = len(w_mat[0])
    iw = []
    with open(vec_path + str(year) + "-w" + extension, "w") as fp:
        print >> fp, str(vocab_size), str(dim)
        for i, w in enumerate(vocab_list):
            print >> fp, w.encode("utf-8"), " ".join(map(str, w_mat[0, :]))


if __name__ == "__main__":
    parser = ArgumentParser("Retrieving vocab and numpy files to txt")
    parser.add_argument("vec_path", help="Vectors path with the prefix file name")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=4)
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--year-inc", type=int, default=10)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, args.vec_path]) for i in range(args.workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()