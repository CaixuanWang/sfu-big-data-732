from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import random

def get_iterations(count):
    iterations = 0
    random.seed()
    for i in range(len(count)):
        sum = 0.0
        while(sum<1):
            sum +=random.random()
            iterations += 1
    return iterations


def main(inputs):
    # main logic starts here
    samples = int(inputs)
    partitions = 10000
    batches = sc.parallelize(range(samples),numSlices = partitions).glom()
    iterations = batches.map(get_iterations)
    iterations = iterations.reduce(lambda x,y: x+y)
    print(iterations/samples)



if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)

# 10000 2.71823931 real    0m30.209s user    1m10.095s sys 0m16.256s
# 1000  2.71836023 real    0m17.398s user    0m20.747s sys 0m1.559s
# 100   2.71843099 real    0m16.437s user    0m19.673s sys 0m0.815s
# 200   2.7184224  real    0m16.722s user    0m18.593s sys 0m0.920s
# 90    2.71825842 real    0m16.552s user    0m18.356s sys 0m0.786s
# 50    2.71828049 real    0m17.190s user    0m15.490s sys 0m0.769s
# 40    2.71818946 real    0m17.301s user    0m18.142s sys 0m0.729s
# 30    2.71835572 real    0m16.396s user    0m14.104s sys 0m0.727s
# 20    2.71845941 real    0m15.741s user    0m13.316s sys 0m0.662s
# 12    2.71833374 real    0m17.612s user    0m16.033s sys 0m0.804s
# 10    2.71832187 real    0m16.305s user    0m13.248s sys 0m0.658s
#  6    2.71830709 real    0m16.434s user    0m11.270s sys 0m0.609s
#  5    2.71818975 real    0m17.675s user    0m12.555s sys 0m0.615s
#  2    2.71832239 real    0m31.828s user    0m9.542s  sys 0m0.509s
#  1    2.71847052 real    0m57.901s user    0m10.417s sys 0m0.570s