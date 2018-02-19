import subprocess

from argparse import ArgumentParser
from ioutils import mkdir

VOCAB_FILE = "{year:d}.vocab"
INPUT_FILE = "{year:d}.txt"
SAVE_FILE = "{year:d}"

def train_years(years, in_dir, out_dir, workers, model, lr, dim, epoch, bucket, loss):
    for i, year in enumerate(years):
        print "Running year", year
        subprocess.call(['./fasttextf/fasttext',
                model,
                '-input', in_dir + INPUT_FILE.format(year=year),
                '-output', out_dir + SAVE_FILE.format(year=year) + "-w",
                '-lr', str(lr),
                '-dim', str(dim),
                '-ws', '1',
                '-epoch', str(epoch),
                '-minCount', '1',
                '-neg', '5',
                '-loss', loss,
                '-bucket', str(bucket),
                '-minn', '3',
                '-maxn', '6',
                '-thread', str(workers),
                '-t', '1e-5',
                '-lrUpdateRate', '100',
                '-verbose', '2'])

if __name__ == "__main__":
    parser = ArgumentParser("Runs fasttext embeddings for years")
    parser.add_argument("in_dir", help="Directory contains pairs of words.")
    parser.add_argument("out_dir")
    parser.add_argument("--dim", type=int, default=300)
    parser.add_argument("--epoch", type=int, default=1)
    parser.add_argument("--model", default="skipgram")
    parser.add_argument("--workers", type=int, default=50)
    parser.add_argument("--lr", type=float, default=0.025)
    parser.add_argument("--bucket", type=int, default=2000000)
    parser.add_argument("--loss", default="ns")
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--year-inc", type=int, default=1)
    args = parser.parse_args()
    out_dir = args.out_dir + "/" + str(args.dim) + "/"
    mkdir(out_dir)
    years = range(args.start_year, args.end_year + 1, args.year_inc)
    train_years(years, args.in_dir + "/", out_dir, args.workers, args.model, args.lr, args.dim, args.epoch, args.bucket, args.loss)
