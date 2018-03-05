from multiprocessing import Queue, Process

from argparse import ArgumentParser
import os

"""
print word vectors given years range and and queries file
"""
MODEL_FILE = "{year:d}-w.bin"

def print_vectors(year, vec_dir, out_file, queries_file, dim):
    print "Print word vectors for year", year
    vocab_size = os.popen("wc -l " + queries_file + " | awk '{ print $1 }'").read().strip()
    os.system("echo '" + vocab_size + " " + str(dim) + "' > " + out_file + str(year) + "-w.vec")
    os.system("./fasttextf/fasttext print-word-vectors " + vec_dir + MODEL_FILE.format(year=year)
        + " < " + queries_file + " >> " + out_file + str(year) + "-w.vec")

def worker(proc_num, queue, vec_dir, out_file, queries_file, dim):
    while True:
        if queue.empty():
            break
        year = queue.get()
        print proc_num, "Print vectors for year ", year
        print_vectors(year, vec_dir, out_file, queries_file, dim)

def run_parallel(workers, years, vec_dir, out_file, queries_file, dim):
    queue = Queue()
    for year in years:
        queue.put(year)
    procs = [Process(target=worker, args=[i, queue, vec_dir, out_file, queries_file, dim]) for i in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    """
    for year in years:
        print_vectors(year, vec_dir, out_file, queries_file, dim)
    """

if __name__ == "__main__":
    parser = ArgumentParser("print word vectors given year and queries file")
    parser.add_argument("vec_dir", help="Directory contains word vectors")
    parser.add_argument("out_file", help="Output vectors file")
    parser.add_argument("queries_file", help="File contains words to print vectors for")
    parser.add_argument("--dim", type=int, default=300)
    parser.add_argument("--workers", type=int, help="Number of processes to spawn", default=8)
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    run_parallel(args.workers, years, args.vec_dir, args.out_file, args.queries_file, args.dim)
