'''
Cracking the code interview.

Description: This pipeline read multiple text files ( python scripts) from a folder,
             extract all text from files as a string line,
             transform that string line,
             get the first triple quoted comments per line,
             load that new data collection as a text file.

Each python script in the folder contains a tripled quoted comment,
those comments are the question interview, the problem description.

We want our pipeline
extract all the problem descriptions
and type them in a txt file.

'''

import apache_beam as beam
import os
import re
from apache_beam.options.pipeline_options import PipelineOptions

input_path  = '/home/art/data/craking_py/input/*.py'
output_path = '/home/art/data/craking_py/output/result.txt'

options     = PipelineOptions(
    runner        = 'DirectRunner',
    project       = 'Transcriber',
    job_name      = 'job_comment_Xtractor' ,
    temp_location = '/home/art/data/tmp'
)

# Concatenate all the values, all the rows for one file are joined in one row.
def concat_lines( values ):
    s = ''
    for i in values:
        i = i.replace( '\n', ' ' )
        s = s + i

    return s


def crop_file_name( tuple ):
    dir, file_name = os.path.split( tuple[ 0 ] )
    return ( file_name, tuple[ 1 ] )

str_pattern = "'''.+'''"
pattern = re.compile(str_pattern)

def get_comment( line ):
    comment         = ''
    line            = line.replace('\n', ' ')
    results   = pattern.search( line )

    if results == None:
        print( 'No matches in regex' )
        comment = line[ 0:10 ]
    else:
        comment = results.group(0)

    return comment

with beam.Pipeline( ) as pipeline:
    lines = ( pipeline

            | beam.Create( [ input_path ] )

            | 'read input files' >> beam.io.ReadAllFromText(
                strip_trailing_newlines= True,
                with_filename=True)

            | 'remove newline delimiter' >> beam.CombinePerKey( concat_lines )
            | 'crop file name'           >> beam.Map( lambda t: crop_file_name( t ) )

            | 'remove key and get value' >> beam.Map( lambda t: get_comment( t[1] ) )
            | 'extract comments'         >> beam.Map( lambda line: get_comment( line ) )
            | 'write output file'        >> beam.io.WriteToText(  output_path )
            )

print( 'End.' )