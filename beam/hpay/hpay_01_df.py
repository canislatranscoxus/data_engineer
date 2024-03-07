'''
Description: In this pipeline we extract data from pay.csv,
             make some transformations and load to Data Lake.

             We use Apache Beam dataframes !

             We select these columns
            * pay_id
            * order_id
            * amount
            * status
            * payment_method
            * payment_timestamp
'''
# import libraries
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io             import read_csv

# set some vars
in_path  = '/home/art/data/hpay/in/pay.csv'
out_path = '/home/art/data/hpay/out/pay_01.csv'

options = PipelineOptions(
    runner        = 'DirectRunner',
    project       = 'Hpay',
    job_name      = 'Hpay_payments',
    temp_location = '/home/art/data/tmp'
)

fields = [ 'pay_id',	'order_id',	'amount', 'status', 'payment_method', 'payment_timestamp']

# Do our pipeline
with beam.Pipeline( options = options ) as pipeline:
    df = ( pipeline
    | 'create PCollection' >> read_csv( in_path )
    )

    # SELECT columns ...
    # FROM   df
    #df = df.filter( items = fields )   # option 1
    df = df[ fields ]                   # option 2, more elegant.

    print( 'df columns: ', df.columns )

    # remove bad rows
    df = df[ df['pay_id'].isnull() == False ]  # good rows

    # make transformations
    df[ 'status'        ] = df[ 'status' ].str.upper()
    df[ 'payment_method'] = df['payment_method'].apply( lambda s: s.upper() )

    # Load clean data to Data Lake.
    df.to_csv( out_path, index = False )


print( '\n End.' )