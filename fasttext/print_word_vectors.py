from argparse import ArgumentParser
import os

"""
print word vectors given year and queries file
"""
MODEL_FILE = "{year:d}-w.bin"

def main(year, vec_dir, out_file, queries_file, dim):
    print "Print word vectors for year", year
    vocab_size = os.popen("wc -l " + queries_file + " | awk '{ print $1 }'").read().strip()
    os.system("echo '" + vocab_size + " " + str(dim) + "' > " + out_file + "-" + str(year) + "-w.vec")
    os.system("./fasttextf/fasttext print-word-vectors " + vec_dir + MODEL_FILE.format(year=year)
        + " < " + queries_file + " >> " + out_file + "-" + str(year) + "-w.vec")

if __name__ == "__main__":
    parser = ArgumentParser("print word vectors given year and queries file")
    parser.add_argument("year", type=int)
    parser.add_argument("vec_dir", help="Directory contains word vectors")
    parser.add_argument("out_file", help="Output vectors file")
    parser.add_argument("queries_file", help="File contains words to print vectors for")
    parser.add_argument("--dim", type=int, default=300)
    args = parser.parse_args()
    main(args.year, args.vec_dir, args.out_file, args.queries_file, args.dim)
