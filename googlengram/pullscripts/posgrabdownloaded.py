import re
import os
import subprocess
import argparse
import collections

import ioutils

VERSION = '20120701'
TYPE = '1gram'
POS = re.compile('[^_]+_[A-Z]+')

def main(in_dir, out_dir, years):
    year_counts = {}
    year_doc_counts = {}
    year_pos = {}
    for year in years:
        year_pos[year] = {}
        year_counts[year] = {}
        year_doc_counts[year] = {}

    print "Start loop"
    for zipped_file in os.listdir(in_dir):
        if not os.path.isfile(in_dir + "/" + zipped_file) or not zipped_file.endswith((".gz")):
            continue

        name = zipped_file.split(".gz")[0]
        print  "Unzipping", name
        subprocess.call(['gunzip', '-f', in_dir + "/" + name + '.gz', '-d'])

        print  "Going through", name
        with open(in_dir + "/" + name) as f:
            for l in f:
                try:
                    split = l.strip().split('\t')
                    if not POS.match(split[0]):
                        continue
                    count = int(split[2])
                    if count < 10:
                        continue
                    word_info = split[0].split("_") 
                    pos = word_info[-1]
                    word = word_info[0].decode('utf-8').lower()
                    word = word.strip("\"")
                    word = word.split("\'s")[0]
                    year = int(split[1])
                    doc_count = int(split[3])
                    if not year in years:
                        continue
                    if not word in year_counts[year]:
                        year_counts[year][word] = 0
                        year_doc_counts[year][word] = 0
                        year_pos[year][word] = collections.Counter() 
                    year_counts[year][word] += count 
                    year_doc_counts[year][word] += doc_count 
                    year_pos[year][word][pos] += count
                except UnicodeDecodeError:
                     pass

        print "Deleting", name
        try:
            os.remove(in_dir + "/" + name)
            os.remove(in_dir + "/"+ name + '.gz')
        except:
            pass

    print "Writing..."
    for year in years:
        ioutils.write_pickle(year_counts[year], out_dir + str(year) + "-counts.pkl")
        ioutils.write_pickle(year_doc_counts[year], out_dir + str(year) + "-doc_counts.pkl")
        ioutils.write_pickle(year_pos[year], out_dir + str(year) + "-pos.pkl")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pulls and saves unigram data.")
    parser.add_argument("in_dir", help="directory where unzipped raw unigrams stored")
    parser.add_argument("out_dir", help="directory where data will be stored")
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=2000)
    args = parser.parse_args()
    years = range(args.start_year, args.end_year + 1)
    ioutils.mkdir(args.out_dir)
    main(args.in_dir, args.out_dir + "/", years) 
