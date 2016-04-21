import gzip
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', help='path to input data')
    parser.add_argument('-o', help='path to output data')
    return parser.parse_args()

def parse(path):
    g = gzip.open(path, 'rb')
    for row in g:
        yield eval(row)
