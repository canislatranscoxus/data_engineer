'''
To see how to solve this,
see the singers.ipnb
 notebook in this folder

'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as func

class ChargePointsETLJob:
    input_path = 'data/input/electric-chargepoints-2017.csv'
    output_path = 'data/output/chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("ElectricChargePointsETLJob")
                                          .getOrCreate())

    def extract(self):
        df = self.spark_session.read.csv( self.input_path )
        return df

    def transform(self, df):
        df.createOrReplaceTempView( 'df' )

        query = ''' select CPID as chargepoint_id, 
        round( MAX( PluginDuration ), 2 )  as max_duration,  
        round( AVG( PluginDuration ), 2) as avg_duration 
        FROM df
        group by 1'''

        df2 = self.spark_session.sql( query )
        return df2

    def load(self, df):
        df.write.parquet( self.output_path )

    def run(self):
        self.load(self.transform(self.extract()))
