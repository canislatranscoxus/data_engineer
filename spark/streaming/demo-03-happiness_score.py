'''
from course: Conceptualizing the Processing
Model for Apache Spark
Structured Streaming

usage:
   * open terminal 1, and run net cat like this
   nc -lk 9999

   * open terminal 2, run our spark streaming script
   spark-submit demo-03-happiness_score.py localhost 9999

   * goto terminal 1, type some data
   EGY Africa 4
   CAM Africa 3
   KEN Africa 5

   USA America 8
   MEX America 7
   BRZ America 9

   KOR Asia 6
   CHI Asia 4
   JAP Asia 5

   SWD Europe 9
   ITA Europe 9
   NOR Europe 10
'''

import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split



def main():
	if len(sys.argv) != 3:
		print('Usage: spark-submit happiness_score.py <host> <port>',file = sys.stderr)
		exit(-1)

	host = sys.argv[1]
	port = int(sys.argv[2])

	sparkSession = SparkSession\
		.builder\
		.appName('Calculating average happiness score')\
		.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")

	readStream = sparkSession\
			.readStream\
			.format('socket')\
			.option('host', host)\
			.option('port', port)\
			.load()


	readStreamDf = readStream.selectExpr("split(value, ' ')[0] as Country",\
										 "split(value, ' ')[1] as Region", \
										 "split(value, ' ')[2] as HappinessScore")

	readStreamDf.createOrReplaceTempView('happiness')

	
	averageScoreDf = sparkSession.sql("""SELECT Region, AVG(HappinessScore) as avgScore
								         FROM Happiness GROUP BY Region""")
	

	query = averageScoreDf \
		.writeStream \
		.outputMode('complete') \
		.format('console') \
		.start() \
		.awaitTermination()


if __name__ == '__main__':
	main()