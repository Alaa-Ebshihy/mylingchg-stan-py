"""
contains some common functions can be used by multiple classes
"""
import numpy as np
from argparse import ArgumentParser

from ioutils import load_pickle, write_pickle

def text2numpy(dir, freqs, year):
    iw = []
    with open(dir + str(year) + "-w") as fp:
        info = fp.readline().split()
        vocab_size = int(info[0])
        dim = int(info[1])
        w_mat = np.zeros((vocab_size, dim))
        for i, line in enumerate(fp):
            line = line.strip().split()
            iw.append(line[0].decode("utf-8"))
            if freqs[iw[-1]] >= 500:
                w_mat[i,:] = np.array(map(float, line[1:]))
    np.save(dir + str(year) + "-w.npy", w_mat)
    write_pickle(iw, dir + str(year) + "-vocab.pkl")