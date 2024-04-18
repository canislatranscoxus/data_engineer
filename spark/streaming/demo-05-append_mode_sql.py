'''
from course: Conceptualizing the Processing
Model for Apache Spark
Structured Streaming

usage:
   * open a terminal run our spark streaming script

   spark-submit demo-05-append_mode_sql.py

'''

from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	sparkSession = SparkSession \
		.builder.master('local') \
		.appName('Projections in append mode')\
		.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField('Date', StringType(), True),
						 StructField('Open', StringType(), True),
						 StructField('High', StringType(), True),
						 StructField('Low', StringType(), True),
						 StructField('Close', StringType(), True),
						 StructField('Adjusted Close', StringType(), True),
						 StructField('Volume', StringType(), True),
						 StructField('Name', StringType(), True)
						 ])

	input_file_path = '/home/art/data/stock_data'
	stockPricesDf 	= sparkSession \
			.readStream \
			.option('header', 'true') \
			.schema(schema) \
			.csv( input_file_path )



	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)


	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())


	stockPricesDf.createOrReplaceTempView('stock_prices')


	selectDf = sparkSession.sql("""SELECT Name, Date, High, Low
								   FROM stock_prices
								   WHERE High - Low > 10""")  
					

	query = selectDf \
			.writeStream \
			.outputMode('append') \
			.format('console') \
			.option('truncate', 'false') \
			.option('numRows', 5) \
			.start() \
			.awaitTermination()



if __name__ == '__main__':
	main()




	