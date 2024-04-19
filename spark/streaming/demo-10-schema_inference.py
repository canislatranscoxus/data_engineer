'''
from course: Conceptualizing the Processing
Model for Apache Spark
Structured Streaming

usage:
   * in a terminal run our spark streaming script

   spark-submit demo-10-schema_inference.py

'''

from pyspark.sql import SparkSession, Row

from pyspark.sql import SQLContext
from pyspark import SparkContext


def main():
	
	sparkSession = SparkSession \
		.builder \
		.appName('Schema inference')\
		.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	input_file_path = '/home/art/data/stock_data'
	stockPricesDf = sparkSession.read \
		.format('csv') \
		.option('header', 'true') \
		.option('inferSchema', 'true') \
		.option('mode', 'DROPMALFORMED') \
		.load( input_file_path )


	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)


	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())

	stockPricesDf.select('Date', 'Name', 'Adj Close').show()


	stockPricesDf.groupBy('Name').count().show()


	stockPricesDf.createOrReplaceTempView('stock_prices')


	query = sparkSession.sql("""SELECT Name, avg(Close) 
						        FROM stock_prices
						        GROUP BY Name""")

	query.show()

	
	
if __name__ == '__main__':
	main()


	
