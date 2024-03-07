'''

Description: In this pipeline we extract data from pay.csv,
             make some transformations such as cleaning, select the columns
            * pay_id
            * order_id
            * amount
            * status
            * payment_method
            * payment_timestamp

            and load into Data Lake.
'''
# import libraries
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

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
out_path = '/home/art/data/hpay/out/pay_00.csv'

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

# Do our pipeline
with beam.Pipeline( options = options ) as pipeline:
    lines = ( pipeline
        | 'create PCollection'  >> beam.io.ReadFromCsv( in_path ).with_output_types(Payment)

        #| 'select columns'  >> beam.Select(
        #            'pay_id'
        #            , 'order_id'
        #            , 'amount'
        #            , 'status'
        #            , 'payment_method'
        #            , 'payment_timestamp'
        #        )
        | 'select columns' >> beam.Select( *fields )

        | 'save output as csv' >> beam.io.WriteToCsv(out_path)
    )



    #r = ( items
    #      | 'save output as csv' >> beam.io.WriteToCsv(out_path)
    #
    #)

    #| 'convert to Rows'     >>
    #| 'select columns'      >>
    #| 'remove bad rows'    >> beam


    # lines is of type  'apache_beam.pvalue.PCollection'
    print( 'type of lines: ', type( lines ) )
    #print('type of items: ', type(items))


print( 'End.' )