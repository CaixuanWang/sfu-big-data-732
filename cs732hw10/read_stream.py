from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions as f, types

def main(topic):
    # load spark
    messages = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
            .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    # split the data
    values_split = f.split(values['value'], ' ')
    # get x and y
    values_x_y = values.withColumn('x', values_split.getItem(0)).withColumn('y', values_split.getItem(1))
    # get other value
    values_all = values_x_y.withColumn('1', f.lit(1)).withColumn('xy',values_x_y['x']*values_x_y['y']).withColumn('x_square',values_x_y['x']*values_x_y['x'])
    # take sums
    sums = values_all.select(f.sum('x').alias('sum_x'), f.sum('y').alias('sum_y'), f.sum('xy').alias('sum_xy'), f.sum('1').alias('n'), f.sum('x_square').alias('sum_x_square'))
    # find beta and alpha
    beta = (sums['sum_xy']-sums['sum_x']*sums['sum_y']/sums['n'])/(sums['sum_x_square']-(sums['sum_x']**2)/sums['n'])
    alpha = sums['sum_y']/sums['n'] - beta * sums['sum_x']/sums['n']
    # get result and stop
    result = sums.withColumn('beta', beta).withColumn('alpha', alpha).drop('n','sum_x','sum_y','sum_xy','sum_x_square')
    stream = result.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(50)

if __name__ == '__main__':
    # build application
    spark = SparkSession.builder.appName('read_stream').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  
    topic = sys.argv[1]
    main(topic)