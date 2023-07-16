from IPython.display   import display, HTML

import findspark
findspark.init()

import pyspark

from pyspark           import SparkContext
from pyspark.streaming import StreamingContext
from datetime          import datetime


if  __name__ == '__main__':
    sc = SparkContext( appName = 'Streaming error count' )
    ssc = StreamingContext( sc, 10 )
    ssc.checkpoint( '/home/art/data/my_checkpoint' )

    host = 'localhost'
    port = 9999
    lines = ssc.socketTextStream( host, port  )

    counts = lines.flatMap( lambda line: line.split(' ') ) \
                  .filter ( lambda word: 'ERROR' in word ) \
                  .map    ( lambda word: (word, 1 )      ) \
                  .reduceByKey( lambda a, b: a + b )

    counts.pprint()
    ssc.start()
    ssc.awaitTermination()