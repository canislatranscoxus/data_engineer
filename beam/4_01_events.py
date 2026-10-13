'''
Description: this script read a json file.
             The input file is read as text and each line represent string dictionary in a json shape.
             print each json dictionary, and
             save the final output to atxt file.

'''

import json
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file = '/home/art/data/in/events.json'
output_file = '/home/art/data/out/4_01.txt'
pattern = r'[A-Za-z\']+'

options = PipelineOptions(
    runner='DirectRunner',
    project='baywatch',
    job_name='job_feed_sharks',
    temp_location='/home/art/data/tmp'
)

with beam.Pipeline(options=options) as pipeline:
    lines = (
             pipeline
             | 'Read from file'  >> beam.io.ReadFromText( input_file )

              #| 'print debug' >> beam.Map( lambda obj: print( type( obj ) ) or obj )

             #| 'get dic' >> beam.Map(lambda line: json.loads( line )  )
             | 'get dic' >> beam.Map( json.loads )

             | 'print output'  >> beam.Map( lambda dic: print( dic ) or dic )
             | 'save as txt'   >> beam.io.WriteToText( output_file, shard_name_template = '' )

             )

print('\n End.')
