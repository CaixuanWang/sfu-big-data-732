from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    word_json = text.map(json.loads)
    pair = word_json.map(set_pairs)
    pairs = pair.reduceByKey(add_pairs)
    pairmean = pairs.map(get_averages)
    outputdata = pairmean.map(json.dumps)
    outputdata.saveAsTextFile(output)

def set_pairs(word_json):
    key = word_json['subreddit']
    value = (1,word_json['score'])
    return (key,value)

def add_pairs(x,y):
    return ((x[0]+y[0]),(x[1]+y[1]))

def get_averages(pair):
    mean = pair[1][1]/pair[1][0]
    return (pair[0],mean)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
