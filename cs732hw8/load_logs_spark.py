from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
import re, uuid
from datetime import datetime
from pyspark.sql.functions import col

def disassemble(line):
    # compile it
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    line_spilted =  line_re.split(line)
    # test if length is right
    if len(line_spilted) == 6:
        # convert date time
        date_cols = datetime.strptime(line_spilted[2],'%d/%b/%Y:%H:%M:%S')
        return (line_spilted[1],date_cols,line_spilted[3],int(line_spilted[4]))

def main(inputs, keyspace, table):
    # Read data
    server = sc.textFile(inputs).repartition(80)
    # Run disassemble and make sure data is usable
    server_dis = server.map(disassemble).filter(lambda x: x is not None)
    # server_dis = server_dis.map(date_convert)
    # build schema
    log_schema = types.StructType([
    types.StructField('host', types.StringType()),
    types.StructField('datetime', types.DateType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.IntegerType()),
    ])
    # Use schema to build DataFrame
    server_log = spark.createDataFrame(server_dis, schema = log_schema)
    # Select useful parts
    # take uuid column function
    uuid_cols=functions.udf(lambda : str(uuid.uuid4()),types.StringType())
    # select columns and add uuid
    server_logs = server_log.select(server_log['host'],server_log['datetime'],server_log['path'],server_log['bytes']).withColumn('id',uuid_cols())
    # write
    server_logs.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true") \
    .options(table=table, keyspace=keyspace).save()


if __name__ == '__main__':
    # set cluster
    cluster_seeds = ['node1.local', 'node2.local']
    # build application
    spark = SparkSession.builder.appName('Spark Cassandra example') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs, keyspace, table)