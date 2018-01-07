"""
evaluator for year range
"""
import subprocess
from argparse import ArgumentParser

def eval_main(years, vec_dir, test_path, word_path, num_context, type):
	for year in years:
		print 'Evaluating for year ' + str(year)
		subprocess.call(["python",
					'-m', 'ws_eval',
                    vec_dir + "/" + str(year),
                    test_path,
                    '--word-path', word_path,
                    '--num-context', str(num_context),
                    '--type', type])

if __name__ == '__main__':
    parser = ArgumentParser("Run word similarity benchmark")
    parser.add_argument("vec_dir", help="Directory to word vectors")
    parser.add_argument("test_path", help="Path to test data")
    parser.add_argument("--start-year", type=int, help="", default=1800)
    parser.add_argument("--end-year", type=int, help="", default=2000)
    parser.add_argument("--year-inc", type=int, help="", default=1)
    parser.add_argument("--word-path", help="Path to sorted list of context words", default="")
    parser.add_argument("--num-context", type=int, help="Number context words to use", default=-1)
    parser.add_argument("--type", default="PPMI")
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1, args.year_inc)
    eval_main(years, args.vec_dir, args.test_path, args.word_path, args.num_context, args.type)
