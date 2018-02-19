import numpy as np
from multiprocessing import Queue, Process
from argparse import ArgumentParser

from ioutils import load_pickle, write_pickle

def worker(proc_num, queue, vec_path):
    while True:
        if queue.empty():
            break
        year = queue.get()
        print "Loading data..", year
#        time.sleep(120 * random.random())
        text2numpy(vec_path + "-", year, ".vec")

def text2numpy(vec_path, year, extension):
    iw = []
    with open(vec_path + str(year) + "-w" + extension) as fp:
        info = fp.readline().split()
        vocab_size = int(info[0])
        dim = int(info[1])
        w_mat = np.zeros((vocab_size, dim))
        for i, line in enumerate(fp):
            line = line.strip().split()
            iw.append(line[0].decode("utf-8"))
            w_mat[i,:] = np.array(map(float, line[1:dim+1]))
    np.save(vec_path + str(year) + "-w.npy", w_mat)
    write_pickle(iw, vec_path + str(year) + "-vocab.pkl")

if __name__ == "__main__":
    parser = ArgumentParser("Post-processes SGNS vectors to easier-to-use format. Removes infrequent words.")
    parser.add_argument("vec_path", help="Vectors path with the prefix file name")
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=20)
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--year-inc", type=int, default=1)
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