import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file = '/home/art/data/songs/eye_of_the_tiger.txt'
output_file = '/home/art/data/songs/out_eye_of_the_tiger.txt'


options = PipelineOptions(
    runner          = 'DirectRunner',
    project         = 'baywatch',
    job_name        = 'job_feed_sharks',
    temp_location   = '/home/art/data/tmp'
)

with (beam.Pipeline( options = options ) as pipeline):
    my_output = ( pipeline
    | beam.io.ReadFromText( input_file )
    'extract_words' | beam.FlatMap( lambda line: re.findall( r'[A-Za-z\']+' )   )
    )


print( my_output )
print( '\n End.' )


