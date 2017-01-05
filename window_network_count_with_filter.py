# this is another modification of the network word count example
# it added a filter to consider only lines start with number 16

"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: window_network_wordcount_with_filter.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit window_network_wordcount_with_filter.py localhost 9999`

 To run this on Yarn, again start a Netcat server 
     `$ nc -lk 9999`
 and then run the exmample
    `$ bin/spark-submit --master YARN \
    window_network_wordcount_with_filter.py\
    full_host_name 9999`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingWindowNetworkWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.filter(lambda line: not line.startswith("16"))\
                .flatMap(lambda line: line.split(" "))\
                .map(lambda word: (word, 1))\
                .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
