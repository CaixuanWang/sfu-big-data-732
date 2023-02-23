import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    # Using the providing code
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])

    # Reading csv
    weather = spark.read.format("s3selectCSV").schema(observation_schema).options(compression='gzip').load(inputs)
    # Check if qflag is NULL
    weather = weather.where(weather['qflag'].isNull())
    # Find data that station starts with CA
    weather = weather.where(weather['station'].startswith('CA'))
    # Find data that observation is TMAX
    weather = weather.where(weather['observation'] == 'TMAX')
    # Rename the new column of actual temperature as tmax
    weather = weather.select('*',(weather['value']/10).alias('tmax'))
    # Only take station date and tmax
    weather = weather.select(weather['station'], weather['date'], weather['tmax'])
    # Return a gzip
    weather.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    # Set up input and output
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather ETL S3 select').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)