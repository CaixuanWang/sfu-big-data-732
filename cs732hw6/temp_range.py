import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    # build schema
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
    # Read data
    weather = spark.read.csv(inputs,schema = observation_schema)
    # several filters and cache
    weather_cache = weather.where(weather['qflag'].isNull()).cache()
    # find max and min data
    weather_tmax = weather_cache.where(weather['observation'] == 'TMAX').withColumnRenamed('value', 'max').withColumnRenamed('station', 'stationmax').withColumnRenamed('date', 'datemax')
    weather_tmin = weather_cache.where(weather['observation'] == 'TMIN').withColumnRenamed('value', 'min')
    # join these two tables with same station and date
    weather_max_min = weather_tmax.join(weather_tmin, (weather_tmax['stationmax'] == weather_tmin['station']) & (weather_tmax['datemax'] == weather_tmin['date']))
    # calculate the range and add as column
    weather_range = weather_max_min.select('*',((weather_max_min['max']-weather_max_min['min'])/10).alias('range')).cache()
    # find the max range and rename the column
    weather_range_max = weather_range.groupby('date').max().withColumnRenamed('station', 'stationtmax').withColumnRenamed('date', 'datetmax')
    # join these two tables with same date and max range
    weather_range_final = weather_range.join(weather_range_max, (weather_range['date']==weather_range_max['datetmax'])&(weather_range['range']==weather_range_max['max(range)']))
    # select columns we need and sort them
    weather_range_final = weather_range_final.select(weather_range_final['date'], weather_range_final['station'], weather_range_final['range'])
    weather_sorted = weather_range_final.orderBy('date', 'station')
    # Return a result
    weather_sorted.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp_range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)