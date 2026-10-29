'''
Description: Batch ETL, 1 line no branches, from file to file.
             This Batch use apache beam to read data from a json file and write to a text file.
             The json file represent user events.
'''

import argparse
import time
import logging
import json
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.combiners import CountCombineFn

from CommonLog          import CommonLog
from PerUserAggregation import PerUserAggregation
from Table_events       import Table_events


class Bat_01:



    def parse_json( self, element ):
        row = json.loads( element )
        return CommonLog( **row )


    def to_dict( self, element ):
        return element._asdict()




    def run( self, argv = None ):
        # Command line arguments
        known_args, pipeline_args = self.get_args()

        # This automatically reads --project, --region, --runner, --worker_zone, etc.
        pipeline_options = PipelineOptions( pipeline_args, save_main_session = True )

        # Set a job name (optional, but good practice)
        job_name = f'batch-user-traffic-pipeline-{int(time.time_ns())}'
        # This line now works because GoogleCloudOptions is imported
        pipeline_options.view_as( GoogleCloudOptions ).job_name = job_name


        # Create the pipeline with the corrected options
        with beam.Pipeline( options = pipeline_options ) as p:
            (p

             | 'Read local file' >> beam.io.ReadFromText( known_args.input_path )
             #| 'ReadFromGCS'    >> beam.io.ReadFromText( known_args.input_path )

             | 'ParseJson'       >> beam.Map( self.parse_json ).with_output_types( CommonLog )


             # | 'PerUserAggregations' >> beam.GroupBy( 'user_id' )
             # .aggregate_field('user_id', CountCombineFn(), 'page_views' )
             # .aggregate_field( 'num_bytes'  , sum             , 'total_bytes' )
             # .aggregate_field( 'num_bytes'  , max             , 'max_bytes'   )
             # .aggregate_field( 'num_bytes'  , min             , 'min_bytes'   )
             # .with_output_types(PerUserAggregation)

             | 'Convert to Dict'  >> beam.Map( self.to_dict )
             | 'print output'     >> beam.Map( lambda my_dic: print( my_dic ) or my_dic )


             | 'write to text'    >> beam.io.WriteToText( known_args.output_path, shard_name_template = '' )



             # | 'WriteToBQ'           >> beam.io.WriteToBigQuery(
             #            known_args.table_name,
             #            schema              = Table_events.schema,
             #            create_disposition  = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             #            write_disposition   = beam.io.BigQueryDisposition.WRITE_TRUNCATE
             #        )


             )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


    def get_args( self ):
        # get the args from command line, and return known_args and pipeline_args
        try:
            # Command line arguments
            parser = argparse.ArgumentParser(description='Load from Json into File')

            # We only define arguments that are custom to THIS script's logic.
            parser.add_argument('--input_path' , required = False, help = 'input  path to events.json',
                                default = '/home/art/data/in/events.json' )

            parser.add_argument('--output_path', required = False, help = 'output Path to results',
                                default= '/home/art/data/out/01.txt' )
            #parser.add_argument('--table_name', required=True, help='BigQuery table name')

            # Separate your custom args from the standard Beam args.
            return parser.parse_known_args( None )

        except Exception as e:
            logging.error( 'get_args(), error: {}'.format( e ) )
            raise

    def __init__( self ):
        try:
            print( '' )
        except Exception as e:
            logging.error(  'Bat_01.__init__(), error: {}'.format( e )  )
            raise

if __name__ == '__main__':
    batch = Bat_01()
    batch.run()