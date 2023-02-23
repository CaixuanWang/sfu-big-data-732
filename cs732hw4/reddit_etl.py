from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

# This function is used to set tuple of comment as the result
def set_comment(reddit_json):
    author = reddit_json['author']
    score = reddit_json['score']
    subreddit = reddit_json['subreddit']
    return {'subreddit':subreddit,'score':score,'author':author}


def main(inputs, output):
    # main logic starts here
    # Get inputs and json load it
    text = sc.textFile(inputs)
    reddit_json = text.map(json.loads)
    # Call function to get a formed result
    comments = reddit_json.map(set_comment)
    # Use filter to check the 'e' and also filter comments into positive and negative
    comment_filt_e = comments.filter(lambda a: 'e' in a['subreddit']).cache()
    positive = comment_filt_e.filter(lambda b: b['score'] > 0)
    negative = comment_filt_e.filter(lambda c: c['score'] <= 0)
    # Json dumps and output the final two files
    positive.map(json.dumps).saveAsTextFile(output + '/positive')
    negative.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    # Set up
    conf = SparkConf().setAppName('reddit_etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    # Get input and output
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)