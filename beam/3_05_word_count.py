'''
This example read a list of strings,
count the number of times per each word,
using FlatMap and Combine
and write it to a txt file.

links:
    https://beam.apache.org/get-started/wordcount-example
'''

import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

output_file = '/home/art/data/songs/count_words_3_05.txt'
pattern = r'[A-Za-z\']+'

lines = [
    "Day to night to morning, keep with me in the moment",
    "I'd let you had I known it, why don't you say so?",
    "Didn't even notice, no punches left to roll with",
    "You got to keep me focused, you want it, say so",
    "Day to night to morning, keep with me in the moment",
    "I'd let you had I known it, why don't you say so?",
    "Didn't even notice, no punches left to roll with",
    "You got to keep me focused, you want it, say so",
]

options = PipelineOptions(
    runner          = 'DirectRunner',
    project         = 'baywatch',
    job_name        = 'job_feed_sharks',
    temp_location   = '/home/art/data/tmp'
)

with beam.Pipeline( ) as pipeline:
    lines = ( pipeline
    | 'create PColection' >> beam.Create( lines )
    | 'extract words'     >> beam.FlatMap( lambda line: re.findall( pattern, line ) )
    | 'make key value'    >> beam.Map( lambda word: (word, 1) )
    | 'count each word'   >> beam.CombinePerKey( sum )
    | 'save as txt'       >> beam.io.WriteToText( output_file )
    )

print( '\n End.' )


