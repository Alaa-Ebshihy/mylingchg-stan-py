from argparse import ArgumentParser
from representations.sequentialembedding import SequentialEmbedding
from scipy.stats.stats import spearmanr
import ioutils
"""
This is to measure the dicharonic validity for pairs of words across time
"""


def evaluate_diachronic_accuracy(embedding_path, word_pairs_path, start_year, end_year, year_inc):
    word_pairs_1, word_pairs_2 = ioutils.load_word_pairs(word_pairs_path)
    embeddings = SequentialEmbedding.load(embedding_path, range(start_year, end_year + 1, year_inc))
    stat_sig_count = 0
    pairs_len = len(word_pairs_1)
    print "Getting similarities for", word_pairs_1[0]
    print "Correlation", "\t", "p-value"
    print "-----------------------------"
    for i in range(pairs_len):
        p1 = word_pairs_1[i]
        p2 = word_pairs_2[i]
        time_sims = embeddings.get_time_sims(p1, p2)
        spear_corr = compute_spear_corr(time_sims)
        print "{corr:0.7f}\t{p:0.7f}".format(corr=spear_corr[0], p=spear_corr[1])
        if spear_corr[1] <= 0.05:
            stat_sig_count += 1
    return stat_sig_count * 1.0 / pairs_len


def compute_spear_corr(time_sims):
    years = []
    sims = []
    for year, sim in time_sims.iteritems():
        years.append(year)
        sims.append(sim)
    return spearmanr(sims, years)


if __name__ == "__main__":
    parser = ArgumentParser("Calculate the diachronic accuracy for pairs of words set")
    parser.add_argument("embedding_path", help="Path of word vectors")
    parser.add_argument("word_pairs_path", help="Word pairs file")
    parser.add_argument("start_year", type=int, help="start year (inclusive)")
    parser.add_argument("end_year", type=int, help="end year (inclusive)")
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    args = parser.parse_args()
    print evaluate_diachronic_accuracy(args.embedding_path, args.word_pairs_path, args.start_year, args.end_year,
                                       args.year_inc)
