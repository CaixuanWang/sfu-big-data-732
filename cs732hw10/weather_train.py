import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather_train').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# build schema
tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(model_file, inputs):
    # get the data and cache them
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25],seed=40)
    train = train.cache()
    validation = validation.cache()
    # build SQL statement
    data_sql = SQLTransformer(statement='SELECT today.latitude, today.longitude, today.tmax AS tmax, \
        today.elevation, dayofyear(today.date) AS day_of_year, yesterday.tmax AS yesterday_tmax \
        FROM __THIS__ as today INNER JOIN __THIS__ as yesterday \
        ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station')
    # build model
    assembler = VectorAssembler(inputCols=['latitude','longitude','elevation','day_of_year','yesterday_tmax'], outputCol='features')
    classifier = GBTRegressor(featuresCol='features',labelCol='tmax')
    pipeline = Pipeline(stages=[data_sql, assembler, classifier])
    model = pipeline.fit(train)
    # use the model to make predictions
    predictions = model.transform(train)
    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    # print result
    print('r2 =', r2)
    print('rmse =', rmse)
    model.write().overwrite().save(model_file)

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(model_file, inputs)
