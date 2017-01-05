# this is a modification of the basic network_wordcount.py program
# it added sliding window operation to  generate 
# word counts over the last 30 seconds of data, every 10 seconds.  
"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit window_network_wordcount.py localhost 9999`
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
    # Create a local StreamingContext with batch interval of 10 second
    # you can change the second parameter to other interval 
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
