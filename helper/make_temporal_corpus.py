import ioutils
import re
from nltk.corpus import stopwords
from argparse import ArgumentParser


EXCLUDE_PATTERN = re.compile('[^A-Za-z]+')


def create_comman_vocab(in_dir, ngram_file_suffix, out_dir, out_file_name, years, lang):
    stop_set = set(stopwords.words(lang))
    common_vocab_set = set()

    for year in years:
        file_content_list = ioutils.load_word_list(in_dir + str(year) + ngram_file_suffix)
        words_set = set()
        for line in file_content_list:
            words_line = line.split()
            for w in words_line:
                if not (w.lower().isalpha()) or (w.lower() in stop_set) or (w.lower() in words_set) or (len(w) <= 2):
                    continue
                words_set.add(w.lower())
        # print words_set

        if year != years[0]:
            common_vocab_set = common_vocab_set.intersection(words_set)
        else:
            common_vocab_set = words_set
    # print list(common_vocab_set)
    ioutils.write_list(out_dir + out_file_name, list(common_vocab_set))


if __name__ == "__main__":
    parser = ArgumentParser("Create common corpus from ngrams file")
    parser.add_argument("in_dir", help="Input directory of .ngrams files")
    parser.add_argument("ngram_suffix", help="Suffixes of ngrams files")
    parser.add_argument("out_dir", help="Output directory of common vocab")
    parser.add_argument("out_file_name", help="Output file name")
    parser.add_argument("--start-year", type=int, help="start year (inclusive)", default=1800)
    parser.add_argument("--end-year", type=int, help="end year (inclusive)", default=1990)
    parser.add_argument("--year-inc", type=int, help="year increment", default=10)
    parser.add_argument("--lang", type=str, default="english", help="language")
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    ioutils.mkdir(args.out_dir)
    create_comman_vocab(args.in_dir + "/", args.ngram_suffix, args.out_dir + "/", args.out_file_name, years, args.lang)
