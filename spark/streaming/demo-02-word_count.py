'''
from course: Conceptualizing the Processing
Model for Apache Spark
Structured Streaming

usage:
   * open a terminal and run net cat like this
   nc -lk 9999

   * in another terminal run our spark streaming script
   spark-submit demo-02-word_count.py <host> <port>

usage example:
   spark-submit demo-02-word_count.py locahost 9999



'''


import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def main():
	if len(sys.argv) != 3:
		print('Usage: spark-submit WordCount.py <host> <port>',file = sys.stderr)
		exit(-1)

	host = sys.argv[1]
	port = int(sys.argv[2])

	sparkSession = SparkSession \
		.builder \
		.appName("Word Count")\
		.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")


	readStream = sparkSession \
		.readStream.format('socket') \
		.option('host', host) \
		.option('port', port) \
		.load()

	print("-------------------------------------------------")	
	print("Streaming source ready: ", readStream.isStreaming)

	readStream.printSchema()

	words = readStream.select( \
		explode(split(readStream.value,' ')).alias('word'))


	wordCounts = words.groupBy('word').count() \
			.orderBy('count')


	query = wordCounts \
		.writeStream \
		.outputMode('complete') \
		.format('console') \
		.start() \
		.awaitTermination()



if __name__ == '__main__':
	main()

