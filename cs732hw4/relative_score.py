from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    # Get input and json load it and save a cache
    text = sc.textFile(inputs)
    word_json = text.map(json.loads).cache()
    # Generate pairs and reduce it. Then get averages.
    pair = word_json.map(set_pairs)
    pairs = pair.reduceByKey(add_pairs)
    pairmean = pairs.map(get_averages)
    # Use that cache to get c[subreddit] and join with the averages we get
    commentbysub = word_json.map(lambda c: (c['subreddit'], c))
    comment_join = commentbysub.join(pairmean)
    # get average from the join and sort it and json dumps it. Then save it as output.
    comment_mean = comment_join.map(get_averages_comment)
    comment_mean_sort = comment_mean.sortByKey(ascending = False)
    outputdata = comment_mean_sort.map(json.dumps)
    outputdata.saveAsTextFile(output)

# This function grab json and return a (subreddit, (1, score))
def set_pairs(word_json):
    key = word_json['subreddit']
    value = (1,word_json['score'])
    return (key,value)

# This function adds pairs
def add_pairs(x,y):
    pair_pos1 = x[0]+y[0]
    pair_pos2 = x[1]+y[1]
    return (pair_pos1, pair_pos2)

# This function calculates the averages
def get_averages(pair):
    key = pair[0]
    score = pair[1][1]
    times = pair[1][0]
    mean = score/times
    return (key,mean)

# This function takes a join result and return the (average, author)
def get_averages_comment(comment):
    subreddit, join = comment
    commentbysub, value = join
    return ((commentbysub['score'])/value, commentbysub['author'])

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    # Get input and output
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)