from pyspark import SparkConf, SparkContext
import sys,datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(model_file):
    # Read data
    filt = [('lab', datetime.date(year=2022, month=11, day=18), 49.2771, -122.9146, 330.0, 12.0), \
        ('lab', datetime.date(year=2022, month=11, day=19), 49.2771, -122.9146, 330.0, 12.0)]
    # build dataframe
    data = spark.createDataFrame(filt, schema = tmax_schema)
    model = PipelineModel.load(model_file)
    prediction = model.transform(data)
    prediction=prediction.collect()[0][7]
    # prediction.show()
    print('Predicted tmax tomorrow:', prediction)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('weather_tomorrow').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    model_file = sys.argv[1]
    main(model_file)