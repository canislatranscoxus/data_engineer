'''
Description: In this pipeline we extract 2 csv files in two different PCOllections,
             next we merge them in a single pipeline,
             make some transformations and load to Data Lake.

             This idea can be useful when we want to ingest from different sources in the same pipeline.
             For example, when we need to load from batch and streaming data,
             like the following diagram:

                ┌────────────┐
                │   Batch    │ --->  ┌────────────┐         ┌────────────┐
                └────────────┘       │            │         │            │
                                     │  Pipeline  │ ----->  │  BigQuery  │
                ┌────────────┐ --->  │            │         │            │
                │ Streamimng │       └────────────┘         └────────────┘
                └────────────┘

             We use Apache PCollection and Flatten method.

             We select these columns
            * pay_id
            * order_id
            * amount
            * status
            * payment_method
            * payment_timestamp
links:
https://beam.apache.org/documentation/transforms/python/other/flatten/
'''
# import libraries
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from Payment                              import Payment
from Payment_small                        import Payment_small
from CastFields                           import CastFields


import typing


# set some vars
in_path_ohio  = '/home/art/data/hpay/in/pay_ohio.csv'
in_path_texas = '/home/art/data/hpay/in/pay_texas.csv'
out_path = '/home/art/data/hpay/out/pay_04.csv'

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

clean_row = beam.Row(
    pay_id=None,
    order_id=None,
    amount=None,
    status=None,
    payment_method=None,
    payment_timestamp=None,
    house_id=None,
    created=None,
    year=None,
    m01=None,
    m02=None,
    m03=None,
    m04=None,
    m05=None,
    m06=None,
    m07=None,
    m08=None,
    m09=None,
    m10=None,
    m11=None,
    m12=None
)


def is_good_row( payment ):
    #print('payment: ', type( payment ), payment.pay_id)
    result = payment.pay_id != None
    return result


def line_to_Row( line ):
    a         = line.split( ',' )

    try:
        row = beam.Row(
        pay_id            = int  ( a[  0 ] ),
        order_id          = int  ( a[  1 ] ),
        amount            = float( a[  2 ] ),
        status            = str  ( a[  3 ] ),
        payment_method    = str  ( a[  4 ] ),
        payment_timestamp = str  ( a[  5 ] ),
        house_id          = str  ( a[  6 ] ),
        created           = str  ( a[  7 ] ),
        year              = int  ( a[  8 ] ),
        m01               = int  ( a[  9 ] ),
        m02               = int  ( a[ 10 ] ),
        m03               = int  ( a[ 11 ] ),
        m04               = int  ( a[ 12 ] ),
        m05               = int  ( a[ 13 ] ),
        m06               = int  ( a[ 14 ] ),
        m07               = int  ( a[ 15 ] ),
        m08               = int  ( a[ 16 ] ),
        m09               = int  ( a[ 17 ] ),
        m10               = int  ( a[ 18 ] ),
        m11               = int  ( a[ 19 ] ),
        m12               = int  ( a[ 20 ] )
        )
    except Exception as e:
        # it most be header or noise. Let's use empty Row.
        row = clean_row
    return row


# Do our pipeline
with beam.Pipeline( options = options ) as pipeline:
    ohio  = ( pipeline
              | 'Ohio'  >> beam.io.ReadFromCsv( in_path_ohio ).with_output_types( Payment )
    )
    texas = ( pipeline
              | 'Texas'            >> beam.io.ReadFromText( in_path_texas )
              | 'convert to Rows'  >> beam.Map( lambda line: line_to_Row( line) )
    )

    print( 'type of ohio: ', type( ohio ) )


    merged = (    (ohio, texas)
        | 'merge' >> beam.Flatten()
        | 'select columns' >> beam.Select(*fields)
        | 'get good rows' >> beam.Filter(is_good_row)
        | 'cast pay_id to int' >> beam.ParDo(CastFields())

        #| 'save output as csv' >> beam.io.WriteToCsv( out_path )
        # WriteToCsv fails wafter a Flatten

        | 'print' >> beam.LogElements()
    )
    print( 'type of merged: ', type( merged ) )

print( 'End.' )