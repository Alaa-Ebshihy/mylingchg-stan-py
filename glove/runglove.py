import subprocess

from argparse import ArgumentParser
from ioutils import mkdir

VOCAB_FILE = "{year:d}.vocab"
INPUT_FILE = "{year:d}-pair_counts.shuf.bin"
SAVE_FILE = "{year:d}"

def train_years(years, vocab_dir, count_dir, out_dir, dim, iter, workers, alpha, x_max, eta):
    for i, year in enumerate(years):
        print "Running year", year
        subprocess.call(['./glovef/build/glove',
                '-save-file', out_dir + SAVE_FILE.format(year=year) + "-w",
                '-threads', str(workers),
                '-write-header', '1',
                '-input-file', count_dir + INPUT_FILE.format(year=year),
                '-vector-size', str(dim),
                '-iter', str(iter),
                '-alpha', str(alpha),
                '-x-max', str(x_max),
                '-eta', str(eta),
                '-vocab-file', vocab_dir + VOCAB_FILE.format(year=year),
                '-words-file', vocab_dir + VOCAB_FILE.format(year=year),
                '-contexts-file', vocab_dir + VOCAB_FILE.format(year=year),
                '-verbose', '2'])

if __name__ == "__main__":
    parser = ArgumentParser("Runs sequential word2vec embeddings for years")
    parser.add_argument("vocab_dir", help="Directory with vocab (.vocab).")
    parser.add_argument("count_dir", help="Directory with cooccurrence information (.shuf.bin)")
    parser.add_argument("out_dir")
    parser.add_argument("--dim", type=int, default=300)
    parser.add_argument("--iter", type=int, default=25)
    parser.add_argument("--workers", type=int, default=50)
    parser.add_argument("--alpha", type=float, default=0.75)
    parser.add_argument("--x-max", type=float, default=100.0)
    parser.add_argument("--eta", type=float, default=0.05)
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--year-inc", type=int, default=1)
    args = parser.parse_args()
    out_dir = args.out_dir + "/" + str(args.dim) + "/"
    mkdir(out_dir)
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    train_years(years, args.vocab_dir + "/", args.count_dir + "/", out_dir, args.dim, args.iter, args.workers, args.alpha, args.x_max, args.eta)

