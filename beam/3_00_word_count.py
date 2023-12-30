'''
This example read a list of strings and write it to a txt file.

links:
    https://beam.apache.org/get-started/wordcount-example
'''

import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file = '/home/art/data/songs/eye_of_the_tiger.txt'
output_file = '/home/art/data/songs/out_say_so.txt'


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


with beam.Pipeline( ) as pipeline:
    lines = ( pipeline
    | beam.Create( a )
    | beam.io.WriteToText( output_file )
    )

print( '\n End.' )


