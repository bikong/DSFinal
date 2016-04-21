import pandas as pd
import numpy as np
import gzip
import csv
import string
import gzip
import argparse


review_path = '../data/reviews_Electronics.json.gz'
stopwords_path = './property/stopwords.txt'
sentiment_path = './property/AFINN-111.tsv'
result_path = "./sentiment_score.csv"

stopWord = []
wordRate = {}

with open(stopwords_path) as stopwords:
    stopWord = list(stopwords.read().splitlines())

with open(sentiment_path, 'r') as r:
    tsvread = csv.reader(r, delimiter='\t')
    for row in tsvread:
        wordRate[row[0]] = row[1]


def parse(path):
    g = gzip.open(path, 'rb')
    for row in g:
        yield eval(row)
        
def save(data, header=False):
    df = pd.DataFrame.from_dict(data, orient='index')
    df[['reviewerID', 'asin', 'helpful', 'reviewText', 'overall', 'reviewTime', 'score']].to_csv(result_path, mode='a', header=header, index=False)
    
def process():
    i = 0
    header = True
    buf = {}

    for d in parse(review_path):
        score = sentiment_analysis(d["reviewText"])
        d["score"] = score
        buf[i] = d
        i += 1
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


def sentiment_analysis(review):
    score = 0.0
    review = review.encode('utf-8').lower()

    for punc in string.punctuation:
	review = review.replace(punc, " ")	
   	    
    review = [word for word in review.split() if word not in stopWord]

    for word in review:
   	if wordRate.has_key(word):
   	    score += float (wordRate.get(word))

    if len(review) == 0:
   	score = 0
    else:
   	score = score / len(review)
    return score


if __name__ == '__main__':
    print '\nStart processing...\n'
    process()
    print '\nCompleted!\n'



