from argparse import ArgumentParser

"""
setup queries file to be used in printing word vectors
"""
def main(in_file, out_file):
	# load queries from similarity files
	col1, col2 = load_vocabulary(in_file)

	#write to output file
	with open(out_file, "w") as fp:
		for q in col1:
			print >>fp, q
		for q in col2:
			print >>fp, q

def load_vocabulary(in_file):
    col1 = []
    col2 = []
    with open(in_file) as f:
        lines = f.read().splitlines()
        col1 = [ line.split()[0] for line in lines]
        col2 = [ line.split()[1] for line in lines]
    return col1, col2

if __name__ == "__main__":
    parser = ArgumentParser("Setup queries file to be used in printing word vectors")
    parser.add_argument("in_file", help="File contains pairs of similar words with their similarity percentage")
    parser.add_argument("out_file", help="Output file of queries")
    args = parser.parse_args()
    main(args.in_file, args.out_file)
