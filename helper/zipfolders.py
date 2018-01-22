import subprocess
from argparse import ArgumentParser
import os


def main(in_dir):
    dirs = os.listdir(in_dir)
    for chunk_num in dirs:
        if os.path.isdir(in_dir + chunk_num):
            print "zipping " + chunk_num
            subprocess.call(["tar",
                        '-zcvf',
                        in_dir + chunk_num + ".tar.gz",
                        in_dir + chunk_num])
            subprocess.call(["rm",
                        '-R',
                        in_dir + chunk_num])


if __name__ == '__main__':
    parser = ArgumentParser("Run word similarity benchmark")
    parser.add_argument("path", help="Directory to word vectors")
    args = parser.parse_args()
    main(args.path)
