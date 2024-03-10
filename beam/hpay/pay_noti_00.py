'''
Description: In this pipeline we extract data from pay.csv,
             make some transformations and load to Data Lake.

             We use Apache PCollection.

             We select these columns
            * pay_id
            * order_id
            * amount
            * status
            * payment_method
            * payment_timestamp
links:
https://beam.apache.org/documentation/sdks/python-streaming/
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py
'''
# import libraries
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from Payment_noti       import Payment_noti
from CastFields_noti    import CastFields_noti

import typing




# set some vars
input_topic = 'hpay_noti_msg'
in_path  = '/home/art/data/hpay/in/pay_noti.csv'
out_path = '/home/art/data/hpay/out/pay_noti.csv'

options = PipelineOptions(
    runner        = 'DirectRunner',
    project       = 'Hpay',
    job_name      = 'Hpay_payments',
    temp_location = '/home/art/data/tmp'
)

fields = [    'pay_id'
            , 'order_id'
            , 'amount'
            , 'status' ]

def is_good_row( payment ):
    #print('payment: ', type( payment ), payment.pay_id)

    result = False
    try:
        pay_id = int( payment.pay_id )
        if type( pay_id ) != None:
            return True
    except Exception as e:
        result = False

    return result

# read from CSV
#         | 'create PCollection'  >> beam.io.ReadFromCsv( in_path ).with_output_types( Payment_noti )

# read from PubSub
#         | 'create PCollection'  >> beam.io.ReadFromPubSub( topic = input_topic )


# Do our pipeline
with beam.Pipeline( options = options ) as pipeline:
    rows = ( pipeline
        | 'create PCollection'  >> beam.io.ReadFromCsv( in_path ).with_output_types( Payment_noti )
        | 'select columns'      >> beam.Select( *fields )
        | 'get good rows'       >> beam.Filter( is_good_row )
        #| 'print' >> beam.LogElements()
        | 'cast pay_id to int'  >> beam.ParDo( CastFields_noti() )
        | 'save output as csv'  >> beam.io.WriteToCsv( out_path )
    )

print( 'End.' )