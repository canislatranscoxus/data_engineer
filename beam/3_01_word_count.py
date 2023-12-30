'''
This example read a list of strings,
count the number of times per each word,

and write it to a txt file.

links:
    https://beam.apache.org/get-started/wordcount-example
'''

import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

output_file = '/home/art/data/songs/count_each_words_pardo.txt'

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
    | 'extract words'     >> beam.ParDo(  )
              beam.FlatMap( lambda line: re.findall( pattern, line ) )

    | 'count each word'   >> beam.combiners.Count.PerElement()
    | 'save as txt'       >> beam.io.WriteToText( output_file )
    )

print( '\n End.' )


