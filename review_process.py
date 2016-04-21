import pandas as pd
import numpy as np
import gzip
from utils import *

infile = None
outfile = None

def save(data, header=False):
    df = pd.DataFrame.from_dict(data, orient='index')
    df[['reviewerID', 'asin', 'helpful', 'reviewText', 'overall', 'reviewTime']].to_csv(outfile, mode='a', header=header, index=False)

def run():
    i = 0
    header = True
    buf = {}

    for d in parse(infile):
        buf[i] = d
        i += 1
        print d
        raw_input()
        print(str(i % 100000) + ' read to buffer\r'),
        if i % 100000 == 0:
            print 'Saving buffer to csv...'
            save(buf, header)
            if header:
                header = False
            print '%d saved\n' % i
            buf = {}

    print 'Saving buffer to csv...'
    save(buf, header)
    print '%d saved\n' % i

if __name__ == '__main__':
    args = parse_args()

    infile = args.i
    outfile = args.o

    print '\nStart processing...\n'
    run()
    print '\nCompleted!\n'
