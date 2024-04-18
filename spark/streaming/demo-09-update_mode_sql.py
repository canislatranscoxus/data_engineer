'''
from course: Conceptualizing the Processing
Model for Apache Spark
Structured Streaming

usage:
   * in a terminal run our spark streaming script

   spark-submit demo-09-update_mode_sql.py

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

	stockPricesDf.createOrReplaceTempView('stock_prices')


	maxCloseDf = sparkSession.sql("""SELECT Name, MAX(Close) as MaxClose
								     FROM stock_prices
								     GROUP BY Name""")  


	query = maxCloseDf \
			.writeStream.outputMode('update') \
			.format('console') \
			.option('truncate', 'false') \
			.option('numRows', 30) \
			.start() \
			.awaitTermination()



if __name__ == '__main__':
	main()




	