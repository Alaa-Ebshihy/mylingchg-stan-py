from argparse import ArgumentParser
import ioutils
"""
To evaluate the data against reference list
"""


def get_precision_at_k(ref_list_path, ranked_list_path, k):
    ref_set = set(ioutils.load_word_list(ref_list_path))
    ranked_set = set(ioutils.load_word_list(ranked_list_path)[:k])
    #print ranked_set.intersection(ref_set)
    return (len(ranked_set.intersection(ref_set)) * 1.0) / len(ref_set) * 1.0


if __name__ == "__main__":
    parser = ArgumentParser("Calculate the diachronic accuracy for pairs of words set")
    parser.add_argument("ref_list_path", help="Path of reference list")
    parser.add_argument("ranked_list_path", help="Path of ranked words from most changed")
    parser.add_argument("k", type=int, help="start year (inclusive)")
    args = parser.parse_args()
    print get_precision_at_k(args.ref_list_path, args.ranked_list_path, args.k)