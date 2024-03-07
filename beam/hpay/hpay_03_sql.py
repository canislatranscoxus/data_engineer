'''
Description: In this pipeline we extract data from pay.csv,
             make some transformations and load to Data Lake.

             We use Apache PCollection and SQL.

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
from apache_beam.transforms.sql           import SqlTransform

import typing


class Payment(typing.NamedTuple):
    pay_id            : int
    order_id          : int
    amount            : float
    status            : str
    payment_method    : str
    payment_timestamp : str
    house_id          : str
    created           : str
    year              : int
    m01               : int
    m02               : int
    m03               : int
    m04               : int
    m05               : int
    m06               : int
    m07               : int
    m08               : int
    m09               : int
    m10               : int
    m11               : int
    m12               : int



# set some vars
in_path  = '/home/art/data/hpay/in/pay.csv'
out_path = '/home/art/data/hpay/out/pay_03.csv'

options = PipelineOptions(
    runner        = 'DirectRunner',
    project       = 'Hpay',
    job_name      = 'Hpay_payments',
    temp_location = '/home/art/data/tmp'
)

fields = [    'pay_id'
            , 'order_id'
            , 'amount'
            , 'status'
            , 'payment_method'
            , 'payment_timestamp']

def is_good_row( payment ):
    #print('payment: ', type( payment ), payment.pay_id)
    result = payment.pay_id != None
    return result

query = '''
SELECT 
  CAST( pay_id as integer ) as pay_id  
, order_id
, amount
, UPPER( status         ) as status
, UPPER( payment_method ) as payment_method
, payment_timestamp

FROM    PCOLLECTION
WHERE pay_id is not null
'''

# Do our pipeline
with beam.Pipeline( options = options ) as pipeline:
    rows = ( pipeline
        | 'create PCollection'  >> beam.io.ReadFromCsv( in_path ).with_output_types(Payment)
        #| 'select columns'      >> beam.Select( *fields )
        #| 'get good rows'       >> beam.Filter( is_good_row )
        #| 'cast pay_id to int'  >> beam.ParDo( CastFields() )
        | 'run query'           >> SqlTransform( query )
        | 'save output as csv'  >> beam.io.WriteToCsv( out_path )
    )

print( 'End.' )