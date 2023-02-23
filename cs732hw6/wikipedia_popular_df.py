from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
import re
# add more functions as necessary

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    # use regular expression to find the right name and return the certain part
    name = re.search(r'/pagecounts-(\d{8}-\d{2})', path)
    hour = name.group(1)
    return hour

def main(inputs, output):
    # main logic starts here
    # build schema
    page_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('bytes', types.LongType())
    ])
    # Read data
    page = spark.read.csv(inputs, sep = ' ', schema = page_schema).withColumn('filename', functions.input_file_name())
    # select certain columns
    page = page.select(path_to_hour(page['filename']).alias('hour'), page['language'], page['title'], page['views'])
    # several filters and cache
    page = page.where(page['language'] == 'en')
    page = page.where(page['title'] != 'Main_Page')
    page_cache = page.where(page['title'].startswith('Special:') == False).cache()
    # sort and find the max range and rename the same-name column
    page_max_views = page_cache.groupby(page['hour']).max().withColumnRenamed('hour', 'hourmax').withColumnRenamed('max(views)', 'viewsmax')
    # broadcast join two tables dat with same hour
    page_bdst = page_cache.join(page_max_views.hint('broadcast'), page_max_views['hourmax']==page_cache['hour'])
    # find the same views page and generate new table
    result = page_bdst.filter(page_bdst['views']==page_bdst['viewsmax']).select(page_cache['hour'], page_bdst['title'], page_bdst['views'])
    # sort the table
    result = result.orderBy(result['hour'], result['title'])
    result.explain()
    # generate the output
    result.write.json(output, mode='overwrite')

if __name__ == '__main__':
    spark = SparkSession.builder.appName('wikipedia_popular_df').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
