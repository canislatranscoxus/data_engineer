'''
This example read a list of strings,
count the number of times per each word,
( using ParDo, ReadFromText. WriteToText )
and write it to a txt file.

links:
    https://beam.apache.org/get-started/wordcount-example
'''

import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file  = '/home/art/data/songs/eye_of_the_tiger.txt'
output_file = '/home/art/data/songs/count_each_words_3_04.txt'
pattern = r'[A-Za-z\']+'

class ExtractWords( beam.DoFn ):
    def process(self, element ):
        line = element
        words = re.findall( pattern, line )
        for word in words:
            yield word

# Create pipeline
options = PipelineOptions(
    runner          = 'DirectRunner',
    project         = 'baywatch',
    job_name        = 'job_feed_sharks',
    temp_location   = '/home/art/data/tmp'
)

with beam.Pipeline( ) as pipeline:
    lines = ( pipeline
    | 'create PColection' >> beam.io.ReadFromText ( input_file )
    | 'extract words'     >> beam.ParDo( ExtractWords()  )
    | 'count each word'   >> beam.combiners.Count.PerElement()
    | 'save as txt'       >> beam.io.WriteToText( output_file )
    )

print( '\n End.' )


