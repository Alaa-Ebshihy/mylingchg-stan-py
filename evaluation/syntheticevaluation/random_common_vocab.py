#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""common_vocab.py: Dumps the common vocabulary between a set of text files."""

from argparse import ArgumentParser
from io import open
import ioutils
from scipy.stats import bernoulli

__author__ = "Vivek Kulkarni"
__email__ = "viveksck@gmail.com"

LOGFORMAT = "%(asctime).19s %(levelname)s %(filename)s: %(lineno)s %(message)s"


def get_common_vocab(in_dir, out_dir, out_file_name, in_suffix, years, n_vocab, donor_path, receptor_path):
    common_vocab = None
    for year in years:
        col1, col2 = ioutils.load_word_pairs(in_dir + str(year) + in_suffix)
        file_vocab = set(col1)
        #file_vocab = set(read_corpus_to_list(in_dir + str(year) + in_suffix))
        # f = open(in_dir + str(year) + in_suffix)
        # for line in f:
        #     for sent in nltk.sent_tokenize(line):
        #         for word in nltk.word_tokenize(sent):
        #             file_vocab.add(word)
        if common_vocab is None:
            common_vocab = file_vocab
        else:
            common_vocab = common_vocab & file_vocab
        # f.close()
    data_bern = bernoulli.rvs(size=len(common_vocab), p=float(n_vocab) / len(common_vocab))

    common_vocab_list = list(common_vocab)
    random_common_vocab = set()
    for idx, i in enumerate(data_bern):
        if i == 1:
            random_common_vocab.add(common_vocab_list[idx])
    random_common_vocab = random_common_vocab.union(set(ioutils.load_word_list(donor_path)).union(ioutils.load_word_list(receptor_path)))
    ioutils.write_list(out_dir + out_file_name, list(random_common_vocab))


def read_corpus_to_list(corpus_file_path):
    with open(corpus_file_path) as f:
        return [word for line in f for word in line.split()]


if __name__ == "__main__":
    parser = ArgumentParser("Setup common vocab from all snapshots")
    parser.add_argument("in_dir", help="path to input corpus")
    parser.add_argument("out_dir", help="path to output corpus")
    parser.add_argument("out_file_name", help="name of output file of common vocab")
    parser.add_argument("donor_file_path", help="path of donor")
    parser.add_argument("receptor_file_path", help="path of receptor")
    parser.add_argument("--n-vocab", type=int, help="number of random vocab", default=3000)
    parser.add_argument("--in-suffix", help="suffix used to name", default="")
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1900)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)

    get_common_vocab(args.in_dir + "/", args.out_dir + "/", args.out_file_name, args.in_suffix, years, args.n_vocab,
                     args.donor_file_path, args.receptor_file_path)



