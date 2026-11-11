import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

output_file = '/home/art/data/out/4_02.txt'

consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Fixed: Java Kafka client requires a dot!
    'group.id': 'ninja-beam-group',
    'auto.offset.reset': 'earliest'
}

options = PipelineOptions(
    runner='DirectRunner',
    project='stream_sensor-data',
    job_name='job_sensor-data',
    temp_location='/home/art/data/tmp',
    streaming=True
)

with beam.Pipeline(options=options) as pipeline:
    (
        pipeline
        | 'Read from Kafka' >> ReadFromKafka(
            consumer_config=consumer_config,
            topics=['sensor-data']
        )
        | 'Extract Value & Decode' >> beam.Map(lambda record: record[1].decode('utf-8'))
        | 'Parse JSON' >> beam.Map(json.loads)
        | 'Print & Verify' >> beam.Map(lambda dic: print(f"⚡ Parsed Dict: {dic}") or dic)
    )

print('\nEnd.')