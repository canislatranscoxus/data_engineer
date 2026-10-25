import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file = '/home/art/data/in/events.json'
output_file = '/home/art/data/out/4_00.txt'
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
             | 'extract words'   >> beam.FlatMap( lambda line: re.findall( pattern, line ) )
             | 'count each word' >> beam.combiners.Count.PerElement()  # Outputs: (word, count)

             # 1. FIX: Use Beam's built-in Top transform to sort globally by the count (index 1 of the tuple)
             | 'sort words' >> beam.combiners.Top.Of(10000, key=lambda kv: kv[1])

             # 2. Flatten the sorted list back into individual elements so they write line-by-line
             | 'flatten list' >> beam.FlatMap(lambda x: x)

             # # 3. Format the output cleanly as "word: count"
             | 'format output' >> beam.Map( lambda kv: f"{ kv[0] }: { kv[1] }" )
             | 'print output'  >> beam.Map( lambda line: print( line ) or line )
             | 'save as txt'   >> beam.io.WriteToText( output_file, shard_name_template = '' )

             )

print('\n End.')
