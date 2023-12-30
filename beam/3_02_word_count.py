'''
This example read a list of strings,
 count the total of words in the text,
 and write it to a txt file.

links:
    https://beam.apache.org/get-started/wordcount-example
'''

import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

output_file = '/home/art/data/songs/count_total_of_words.txt'

options = PipelineOptions(
    runner          = 'DirectRunner',
    project         = 'baywatch',
    job_name        = 'job_feed_sharks',
    temp_location   = '/home/art/data/tmp'
)

a = [
    "Day to night to morning, keep with me in the moment",
    "I'd let you had I known it, why don't you say so?",
    "Didn't even notice, no punches left to roll with",
    "You got to keep me focused, you want it, say so",
    "Day to night to morning, keep with me in the moment",
    "I'd let you had I known it, why don't you say so?",
    "Didn't even notice, no punches left to roll with",
    "You got to keep me focused, you want it, say so",
]

pattern = r'[A-Za-z\']+'

with beam.Pipeline( ) as pipeline:
    lines = ( pipeline
    | 'create PColection' >> beam.Create( a )
    | 'extract words'     >> beam.FlatMap( lambda line: re.findall( pattern, line ) )
    | 'count each word'   >> beam.combiners.Count.Globally()
    | 'save as txt'       >> beam.io.WriteToText( output_file )
    )

print( '\n End.' )


