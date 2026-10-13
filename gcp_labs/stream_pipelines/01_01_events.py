# TODO: Add imports
import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner
from apache_beam.options.pipeline_options import WorkerOptions



# ### functions and classes

# TODO: Add parse_json function

# ### main

def run():

    # Command line arguments
    parser = argparse.ArgumentParser(description='Convert Json into BigQuery using Schemas')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--machine_type', required=False, help='Specify machine type')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('my-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner
    options.view_as(WorkerOptions).machine_type = opts.machine_type

    # TODO: Add static input and output strings

    # Table schema for BigQuery
    # TODO: Add table schema
    #table_schema = 'user_id:STRING,ip:STRING,timestamp:STRING,http_request:STRING,lat:STRING,lng:STRING,http_response:STRING,user_agent:STRING,num_bytes:STRING'


    
    table_schema = {
        'fields': [
        
        { 'name' : 'user_id',
          'type' : 'STRING'
        },
        
        { 'name' : 'ip',
          'type' : 'STRING'
        },
        
        { 'name' : 'timestamp',
          'type' : 'STRING'
        },
        
        
        { 'name' : 'http_request',
          'type' : 'STRING'
        },
        
        
        { 'name' : 'lat',
          'type' : 'FLOAT'
        },
        
        { 'name' : 'lng',
          'type' : 'STRING'
        },

        
        { 'name' : 'http_response', 
          'type' : 'INTEGER'
        },

        
        { 'name' : 'user_agent', 
          'type' : 'STRING'
        },
        
        
        { 'name' : 'num_bytes',
          'type' : 'INTEGER'
        },

        ]
    }


    

    # Create the pipeline
    p = beam.Pipeline(options=options)

    
    '''

    Steps:
    1) Read something
    2) Transform something
    3) Write something

    '''

    # TODO: Add transformation steps to pipeline

    project_id  = 'qwiklabs-gcp-04-7c499a023aff'
    input       = 'gs://{}/events.json'.format( project_id )
    output      = '{}:logs.logs'.format( project_id )


    
    lines = ( p
              | "ReadLines" >> beam.io.ReadFromText( input )
              | "Read json string" >> beam.Map( json.loads )


              | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                output,
                schema             = table_schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition  = beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )              

    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()

