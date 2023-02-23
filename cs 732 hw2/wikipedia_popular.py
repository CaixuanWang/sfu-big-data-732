from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia Popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def splitline(line):
    words = line.split(" ")
    return tuple(words)

def add(x, y):
    return x + y

def pair(x):
    return((x[0],(int(x[3]),x[2])))

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
words = text.map(splitline)
word_en = words.filter(lambda x: x[1] == 'en')
word_main = word_en.filter(lambda x: x[2] != "Main_Page")
word_special = word_main.filter(lambda x: not(x[2].startswith("Special:")))
word = word_special.map(pair)
wordcount = word.reduceByKey(max)
wordsort = wordcount.sortBy(get_key)
wordsort.map(tab_separated).saveAsTextFile(output)
print(wordcount.take(10))