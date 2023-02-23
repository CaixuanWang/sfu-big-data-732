from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
# add more functions as necessary

def edge_pair(graph):
    # split twice
    node, out = graph.split(':')
    out_path = out.strip().split(' ')
    # test for '' and yield useful data
    for path in out_path:
        if path != "":
            yield (int(node), int(path))

def main(inputs, outputs, source, destination):
    # main logic starts here
    # Read data 
    graph = sc.textFile(inputs+'/links-simple-sorted.txt')
    # build schema
    graph_schema = types.StructType([
    types.StructField('source', types.IntegerType()),
    types.StructField('node', types.IntegerType())
    ])
    # Pair data and fit in dataFrame then cache it
    graph_edge = graph.flatMap(edge_pair)
    graph_edges = spark.createDataFrame(graph_edge, schema = graph_schema).cache()
    # create dataFrame of initial path
    initial_path = spark.createDataFrame([(source, 0, 0)], ['init_node','init_source', 'distance'])
    # run for loop to keep join
    for i in range(6):
        # join two dataFrame with same node
        visit_path = initial_path.join(graph_edges, (graph_edges['source'] == initial_path['init_node']))
        # update the distance
        updated_path = visit_path.select(visit_path['node'],visit_path['source'], visit_path['distance']).withColumn('distance', 1+visit_path['distance'])
        # subtract the duplicate ones
        clear_path = updated_path.subtract(initial_path)
        # union and cache it
        initial_path = initial_path.unionAll(clear_path).cache()
        # save the iteration and i use csv.
        initial_path.write.csv(outputs + '/iter-' + str(i), mode = 'overwrite')
        # if achieve the destination, stop the loop. about to be 100 determined by data size
        if initial_path.where(initial_path['init_node']==destination).collect():
            break
    # find the min
    initial_path = initial_path.groupby('init_node').min()
    # set up variable
    edge = destination
    result = [destination]
    # use loop to print the result
    while(edge != source):
        # about to be size of the path size of certain node we set
        path = initial_path.where(initial_path['init_node']==edge).collect()
        # if no path condition
        if path == []:
            # this prints nothing
            result = []
            # this prints None in output
            # result = ['None']
            break
        # get the edge
        edge = path[0][1]
        # break loop
        if edge == 0:
            break
        # add the list
        result.append(edge)
    # print result
    result = sc.parallelize(result[::-1])
    result.saveAsTextFile(outputs + '/path')
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('shortest_path').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, outputs, source, destination)
