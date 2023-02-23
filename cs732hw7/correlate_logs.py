from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
import re
# add more functions as necessary

def disassemble(line):
    # compile it
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    line_spilted =  line_re.split(line)
    # test if length is right
    if len(line_spilted) == 6:
        line_pair = (line_spilted[1], line_spilted[4])
        return line_pair

def main(inputs):
    # main logic starts here
    # build schema
    log_schema = types.StructType([
    types.StructField('host', types.StringType()),
    types.StructField('bytes', types.StringType())
    ])
    # Read data
    server = sc.textFile(inputs)
    # Run disassemble and make sure data is usable
    server_dis = server.map(disassemble).filter(lambda x: x is not None)
    # Use schema to build DataFrame
    server_log = spark.createDataFrame(server_dis, schema = log_schema)
    # Change bytes' data type and add count column
    server_cnt = server_log.select(server_log['host'], server_log['bytes'].cast(types.IntegerType())).withColumn('count',functions.lit(1))
    # sum certain columns and rename it
    server_xy = server_cnt.groupby('host').sum().withColumnRenamed('sum(count)','x').withColumnRenamed('sum(bytes)','y')
    # Add six values as new DataFrame
    six_value = server_xy.withColumn('1',functions.lit(1)).withColumn('square_x',functions.pow(server_xy['x'],2)).withColumn('square_y',functions.pow(server_xy['y'],2)).withColumn('x_y',server_xy['x']*server_xy['y'])
    # Sum it and save cache
    six_sums = six_value.groupby().sum().cache()
    # Calculate the three parts for correlate
    upper = six_sums.select(six_sums['sum(1)']*six_sums['sum(x_y)']-six_sums['sum(x)']*six_sums['sum(y)']).collect()
    downer_left = six_sums.select(functions.sqrt(six_sums['sum(1)']*six_sums['sum(square_x)']-functions.pow(six_sums['sum(x)'],2))).collect()
    downer_right = six_sums.select(functions.sqrt(six_sums['sum(1)']*six_sums['sum(square_y)']-functions.pow(six_sums['sum(y)'],2))).collect()
    # Find the result
    result = upper[0][0]/(downer_left[0][0]*downer_right[0][0])
    # test by corr
    # result = server_xy.corr('x','y')
    result_square = result**2
    # generate the output
    print('r = ', result)
    print('r^2 = ', result_square)
    
    
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('correlate_log').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)
