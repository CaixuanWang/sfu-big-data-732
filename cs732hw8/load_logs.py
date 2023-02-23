import sys, gzip, re, uuid, os
from cassandra.cluster import Cluster
from datetime import datetime
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel

def main(inputs, keyspace, table):
    # set cluster and connect keyspace
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    # create batch and take data
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert_data = session.prepare("INSERT INTO %s(host, id, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)"%(table))
    count = 0
    # use loop to get every input
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                # compile as host, date, path, bytes
                logs_dis = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$').split(line)
                # filter useful data
                if len(logs_dis)==6:
                    # convert date
                    datetime_object=datetime.strptime(logs_dis[2],'%d/%b/%Y:%H:%M:%S')
                    # add data by certain type
                    batch.add(insert_data, (logs_dis[1], uuid.uuid4(), datetime_object, logs_dis[3], int(logs_dis[4])))
                    count +=1
                # only do 200 times
                if count==200:
                    session.execute(batch)
                    count = 0
                    batch.clear()
    # close session
    session.shutdown()

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs, keyspace, table)