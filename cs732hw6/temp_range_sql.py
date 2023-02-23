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
    weather.createOrReplaceTempView('weather')
    # several filters and cache
    weather_cache = spark.sql("SELECT * FROM weather WHERE isNull(qflag)").cache()
    weather_cache.createOrReplaceTempView('weather_cache')
    # find max and min data
    weather_tmax = spark.sql("SELECT date,station,value AS max FROM weather_cache WHERE observation = 'TMAX'")
    weather_tmax.createOrReplaceTempView('weather_tmax')
    weather_tmin = spark.sql("SELECT date,station,value AS min FROM weather_cache WHERE observation = 'TMIN'")
    weather_tmin.createOrReplaceTempView('weather_tmin')
    # join these two tables with same station and date
    weather_max_min = spark.sql("SELECT weather_tmin.date, weather_tmin.station, weather_tmin.min, weather_tmax.max FROM weather_tmin JOIN weather_tmax ON weather_tmin.date=weather_tmax.date AND weather_tmin.station=weather_tmax.station")
    weather_max_min.createOrReplaceTempView('weather_max_min')
    # calculate the range and add as column
    weather_range = spark.sql("SELECT date, station, (max-min)/10 AS range FROM weather_max_min").cache()
    weather_range.createOrReplaceTempView('weather_range')
    # find the max range and rename the column
    weather_range_max = spark.sql("SELECT date, max(range) as maxrange FROM weather_range GROUP BY date")
    weather_range_max.createOrReplaceTempView('weather_range_max')
    # join these two tables with same date and max range
    weather_range_final = spark.sql("SELECT weather_range.date, weather_range.station, weather_range.range FROM weather_range JOIN weather_range_max ON weather_range.date=weather_range_max.date AND weather_range.range=weather_range_max.maxrange")
    weather_range_final.createOrReplaceTempView('weather_range_final')
    # select columns we need and sort them
    weather_sorted = spark.sql("SELECT * FROM weather_range_final ORDER BY date, station")
    # Return a result
    weather_sorted.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp_range_sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)