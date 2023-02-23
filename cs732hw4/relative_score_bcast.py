from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    # Get input and json load it and save a cache
    text = sc.textFile(inputs)
    word_json = text.map(json.loads).map(lambda c: (c['subreddit'], c['author'], c['score'])).cache()
    # Generate pairs and reduce it. Then get averages and collect it.
    pair = word_json.map(set_pairs)
    pairs = pair.reduceByKey(add_pairs)
    # There are very little elements collected. The number is around 5.
    pairmean = dict(pairs.map(get_averages).collect())
    # Do the broadcast
    mean_broadcast = sc.broadcast(pairmean)
    # Get the (subreddit, json) format and take averages of it and broadcast
    commentbysub = word_json.map(lambda c: (c[0], c))
    comment_mean = commentbysub.map(lambda b: get_averages_comment(b,mean_broadcast))
    # Sort and json dumps and return output
    comment_mean_sort = comment_mean.sortByKey(ascending = False)
    outputdata = comment_mean_sort.map(json.dumps)
    outputdata.saveAsTextFile(output)

# This function grab json and return a (subreddit, (1, score))
def set_pairs(word_json):
    key = word_json[0]
    value = (1,word_json[2])
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

# This function takes a join and broadcast project and return the (average, author)
def get_averages_comment(commentbysub,averages):
    score = commentbysub[1][2]
    times = averages.value[commentbysub[0]]
    return (score/times, commentbysub[1][1])

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    # Get input and output
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
