import re
import os
import subprocess
import argparse
import collections
import shutil

import ioutils

VERSION = '20120701'
TYPE = '1gram'
POS = re.compile('[^_]+_[A-Z]+')

def remove_tmp_dirs(out_dir, name_list):
    for name in name_list:
        try:
            shutil.rmtree(out_dir + "/" + name)
        except:
            pass 

def merge_year_counts(out_dir, name_list, years):
    for year in years:
        year_counts = {}
        year_doc_counts = {}
        year_pos = {}

        for name in name_list:
            tmp_year_counts = ioutils.load_pickle(out_dir + "/" + name + "/" + str(year) + "-counts.pkl")
            tmp_year_doc_counts = ioutils.load_pickle(out_dir + "/" + name + "/" + str(year) + "-doc_counts.pkl")
            tmp_year_pos = ioutils.load_pickle(out_dir + "/" + name + "/" + str(year) + "-pos.pkl")
            for word, count in tmp_year_counts.iteritems():
                if not word in year_counts:
                    year_counts[word] = 0
                    year_doc_counts[word] = 0
                    year_pos[word] = collections.Counter()
                year_counts[word] += tmp_year_counts[word]
                year_doc_counts[word] += tmp_year_doc_counts[word]
                counter_keys = tmp_year_pos[word].keys()
                for pos in counter_keys:
                    year_pos[word][pos] += tmp_year_pos[word][pos]

        print "Writing merged counts for " + str(year) + " ..."
        ioutils.write_pickle(year_counts, out_dir + str(year) + "-counts.pkl")
        ioutils.write_pickle(year_doc_counts, out_dir + str(year) + "-doc_counts.pkl")
        ioutils.write_pickle(year_pos, out_dir + str(year) + "-pos.pkl")

    print "Deleting temp dirs ..."
    remove_tmp_dirs(out_dir, name_list)


def main(in_dir, out_dir, years):
    name_list = []

    print "Start loop"
    for zipped_file in os.listdir(in_dir):
        if not os.path.isfile(in_dir + "/" + zipped_file) or not zipped_file.endswith((".gz")):
            continue

        tmp_year_counts = {}
        tmp_year_doc_counts = {}
        tmp_year_pos = {}
        for year in years:
            tmp_year_counts[year] = {}
            tmp_year_doc_counts[year] = {}
            tmp_year_pos[year] = {}

        name = zipped_file.split(".gz")[0]
        name_list.append(name)
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
                    if not word in tmp_year_counts[year]:
                        tmp_year_counts[year][word] = 0
                        tmp_year_doc_counts[year][word] = 0
                        tmp_year_pos[year][word] = collections.Counter() 
                    tmp_year_counts[year][word] += count 
                    tmp_year_doc_counts[year][word] += doc_count 
                    tmp_year_pos[year][word][pos] += count
                except UnicodeDecodeError:
                     pass

        print "Writing tmp " + name
        ioutils.mkdir(out_dir + "/" + name)
        for year in years:
            ioutils.write_pickle(tmp_year_counts[year], out_dir + "/" + name + "/" + str(year) + "-counts.pkl")
            ioutils.write_pickle(tmp_year_doc_counts[year], out_dir + "/" + name + "/" + str(year) +  "-doc_counts.pkl")
            ioutils.write_pickle(tmp_year_pos[year], out_dir + "/" + name + "/" + str(year) +  "-pos.pkl")
        
        print "Deleting", name
        try:
            os.remove(in_dir + "/" + name)
            os.remove(in_dir + "/"+ name + '.gz')
        except:
            pass

    print "Merging..."
    merge_year_counts(out_dir, name_list, years)


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
