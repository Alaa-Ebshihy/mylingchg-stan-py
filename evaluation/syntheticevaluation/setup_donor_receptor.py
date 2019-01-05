from argparse import ArgumentParser
import pandas as pd
import sys
from io import open
import nltk
from random import randint
import ioutils


def prepare_donor_receptor_lists(raw_data_file, words_pos_file, donor_out_file, receptor_out_file, n, same_pos):
    print "Reading words and pos tags"
    word_pos_df = pd.read_csv(words_pos_file)
    donor_list, receptor_candidate_list = get_donor_receptor_candidate(raw_data_file, n)
    receptor_list = get_receptor_same_pos(donor_list, receptor_candidate_list, word_pos_df) if same_pos \
        else get_receptor_no_pos(donor_list, receptor_candidate_list)
    print "Dumping donor and receptor lists"
    ioutils.write_list(donor_out_file, donor_list)
    ioutils.write_list(receptor_out_file, receptor_list)


def get_donor_receptor_candidate(raw_data_file, n):
    # encoding = sys.stdout.encoding or 'utf-8'
    # f = open(raw_data_file)
    # fd = nltk.FreqDist()
    # stopwords = set(nltk.corpus.stopwords.words('english'))
    # for line in f:
    #     for sent in nltk.sent_tokenize(line):
    #         for word in nltk.word_tokenize(sent):
    #             fd[word] += 1
    #
    donor_list = []
    receptor_candidate_list = []
    # for w, count in fd.most_common():
    #     if w not in stopwords:
    #         if n > 0:
    #             donor_list.append(w.encode(encoding))
    #             n -= 1
    #         else:
    #             receptor_candidate_list.append(w.encode(encoding))

    words, counts = ioutils.load_word_pairs(raw_data_file)
    for w in words:
        if n > 0:
            donor_list.append(w)
            n -= 1
        else:
            receptor_candidate_list.append(w)

    return donor_list, receptor_candidate_list


def get_receptor_no_pos(donor_list, receptor_candidate_list):
    receptor_list = []
    for d in donor_list:
        print 'donor', d
        receptor_list.append(get_random_receptor(receptor_candidate_list, receptor_list))
    return receptor_list


def get_random_receptor(receptor_candidate_list, receptor_list):
    r = None
    while r is None:
        r_tmp = receptor_candidate_list[randint(0, len(receptor_candidate_list) - 1)]
        if r_tmp in receptor_list:
            continue
        r = r_tmp
        print r
    return r


def get_receptor_same_pos(donor_list, receptor_candidate_list, word_pos_df):
    receptor_list = []
    no_same_pos_list = []
    for d in donor_list:
        r = None
        print 'donor', d
        num_trails = 0
        donor_pos = word_pos_df[word_pos_df.word == d]['POS'].iloc[0]
        donor_same_pos_df = word_pos_df[word_pos_df.POS == donor_pos]
        while r is None and num_trails < (len(donor_same_pos_df) * 10):
            num_trails += 1
            r_tmp = donor_same_pos_df.iloc[randint(0, len(donor_same_pos_df) - 1)]['word']
            if r_tmp in receptor_list or r_tmp in donor_list:
                continue
            r = r_tmp
        if r is None:
            no_same_pos_list.append(d)
            r = get_random_receptor(receptor_candidate_list, receptor_list)
        receptor_list.append(r)
    return receptor_list


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("filename", help="Raw text file name")
    parser.add_argument("words_pos_file", help="Path of csv file of words and pos tags")
    parser.add_argument("donor_out_file", help="Path of donor file output")
    parser.add_argument("receptor_out_file", help="Path of receptor file output")
    parser.add_argument("n", type=int, help="number of donor words")
    parser.add_argument("--samepos", action="store_true")
    args = parser.parse_args()

    prepare_donor_receptor_lists(args.filename, args.words_pos_file, args.donor_out_file, args.receptor_out_file,
                                 args.n, args.samepos)
