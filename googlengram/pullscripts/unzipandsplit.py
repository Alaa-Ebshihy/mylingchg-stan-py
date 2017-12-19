import re
import os
import subprocess
import argparse
from multiprocessing import Process, Queue

import ioutils

VERSION = '20120701'
TYPE = '5gram'
POS = re.compile('.*_[A-Z]+\s.*')
LINE_SPLIT = 100000000
EXCLUDE_PATTERN = re.compile('.*_[A-Z]+[_,\s].*')

def split_main(proc_num, queue, in_dir, out_dir):
    print proc_num, "Start loop"
    
    while True:
        if queue.empty():
            break
        zipped_file = queue.get()
        name = zipped_file.split(".gz")[0]
        subprocess.call(['gunzip', '-f', in_dir + "/" + zipped_file, '-d'])
        print proc_num, "Splitting", name
        subprocess.call(["split", "-l", str(LINE_SPLIT), in_dir + "/" + name, out_dir + "/" +  name + "-"])
        os.remove(in_dir + "/" + name)

def run_parallel(num_processes, in_dir, out_dir):
    queue = Queue()
    ioutils.mkdir(out_dir)

    for zipped_file in os.listdir(in_dir):
        if not os.path.isfile(in_dir + "/" + zipped_file) or not zipped_file.endswith((".gz")):
            continue
        queue.put(zipped_file)
    
    procs = [Process(target=split_main, args=[i, queue, in_dir, out_dir]) for i in range(num_processes)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Unzip raw data and split to chunks")
    parser.add_argument("in_dir", help="directory for zipped raw dataset")
    parser.add_argument("out_dir", help="directory where data will be stored")
    parser.add_argument("num_procs", type=int, help="number of processes to spawn")
    args = parser.parse_args()
    run_parallel(args.num_procs, args.in_dir, args.out_dir) 
