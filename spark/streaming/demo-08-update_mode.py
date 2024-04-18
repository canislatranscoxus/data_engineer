'''
from course: Conceptualizing the Processing
Model for Apache Spark
Structured Streaming

usage:
   * in a terminal run our spark streaming script

   spark-submit demo-08-update_mode.py

'''

from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	sparkSession = SparkSession \
		.builder \
		.appName('Aggregations in update mode')\
		.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField('Date', DoubleType(), True),
						 StructField('Open', DoubleType(), True),
						 StructField('High', DoubleType(), True),
						 StructField('Low', DoubleType(), True),
						 StructField('Close', DoubleType(), True),
						 StructField('Adjusted Close', DoubleType(), True),
						 StructField('Volume', DoubleType(), True),
						 StructField('Name', StringType(), True)
						 ])

	input_file_path = '/home/art/data/stock_data'
	stockPricesDf = sparkSession \
			.readStream \
			.option('header', 'true') \
			.option('maxFilesPerTrigger', 10) \
			.schema(schema) \
			.csv( input_file_path )



	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)


	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())


	averageCloseDf = stockPricesDf \
			.groupBy('Name') \
			.agg({'Close': 'avg'}) \
			.withColumnRenamed('avg(Close)', 'Average Close')


	query = averageCloseDf \
			.writeStream.outputMode('update') \
			.format('console') \
			.option('truncate', 'false') \
			.option('numRows', 30) \
			.start() \
			.awaitTermination()



if __name__ == '__main__':
	main()




	