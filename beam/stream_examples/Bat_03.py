'''
Description: Batch ETL, 2 branches, one branch write to file, the other to database postgres.
             This Batch use apache beam to read data from a json file, the json file represent user events.


Approach 1. Modern library

'''

import argparse
import time
import logging
import json
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms.combiners     import CountCombineFn

from beam_postgres.io                   import WriteToPostgres

from CommonLog          import CommonLog
from PerUserAggregation import PerUserAggregation
from Table_events       import Table_events


class Bat_03:

    postgres_config : None

    def parse_json( self, element ):
        row = json.loads( element )
        return CommonLog( **row )

    def to_dict( self, element ):
        return element._asdict()

    def run( self, argv = None ):
        try:
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

                # read in lines from pipeline to an initial PCollection
                lines = p | 'Read local file' >> beam.io.ReadFromText(known_args.input_path)

                # branch 1 - read and write raw data to file
                (lines
                 # | 'ParseJson'       >> beam.Map( self.parse_json ).with_output_types( CommonLog )
                 # | 'Convert to Dict' >> beam.Map( self.to_dict )
                 # | 'print output'    >> beam.Map( lambda my_dic: print(my_dic) or my_dic )
                 | 'write raw text' >> beam.io.WriteToText(known_args.output_path_raw, shard_name_template='')
                 )

                # branch 2 - read and write to aggregated info to database postgres
                ( lines
                     | 'ParseJson'           >> beam.Map( self.parse_json ).with_output_types( CommonLog )

                     | 'PerUserAggregations' >> beam.GroupBy( 'user_id' )
                     .aggregate_field('user_id', CountCombineFn(), 'page_views' )
                     .aggregate_field( 'num_bytes'  , sum             , 'total_bytes' )
                     .aggregate_field( 'num_bytes'  , max             , 'max_bytes'   )
                     .aggregate_field( 'num_bytes'  , min             , 'min_bytes'   )
                     .with_output_types(PerUserAggregation)

                     | 'Convert to Dict'  >> beam.Map( self.to_dict )
                     | 'print output'     >> beam.Map( lambda my_dic: print( my_dic ) or my_dic )

                     #| 'write to postgres' >> beam.io.WriteToText( known_args.output_path_agg, shard_name_template = '' )
                     | "Write to Postgres" >> WriteToPostgres( **self.postgres_config )
                 )

            logging.getLogger().setLevel(logging.INFO)
            logging.info("Building pipeline ...")
        except Exception as e:
            logging.error( 'Bat_03.run(), error: {}'.format( e ) )
            raise


    def set_postgres_config( self, known_args, pg_group ):
        try:

            # Dynamically extract only the keys belonging to the Postgres group
            self.postgres_config = { action.dest: getattr(known_args, action.dest) for action in pg_group._group_actions }
            print( json.dumps( self.postgres_config, indent = 4 ) )

        except Exception as e:
            logging.error('Bat_03.set_postgres_config(), error: {}'.format(e))
            raise



    def get_args( self ):
        # get the args from command line, and return known_args and pipeline_args
        try:
            # Command line arguments
            parser = argparse.ArgumentParser(description='Load from Json into File')

            # group a) pipeline file paths
            pipeline_group = parser.add_argument_group( 'pipeline paths' )
            pipeline_group.add_argument('--input_path'     , required = False, help = 'input  path to events.json', default = '/home/art/data/in/events.json' )
            pipeline_group.add_argument('--output_path_raw', required = False, help = 'output Path to raw results', default = '/home/art/data/out/01_raw.txt' )

            # group b) Database connection arguments
            pg_group = parser.add_argument_group( 'postgres config' )
            pg_group.add_argument('--host'    , required = False, help = 'host'     , default = '127.0.0.1'         )
            pg_group.add_argument('--port'    , required = False, help = 'port'     , default = '5433'              )
            pg_group.add_argument('--database', required = False, help = 'database' , default = 'db_etl_01'         )
            pg_group.add_argument('--table'   , required = False, help = 'table'    , default = 'events_per_user'   )
            pg_group.add_argument('--user'    , required = False, help = 'user'     , default = 'dev_monkey_01'     )
            pg_group.add_argument('--password', required = False, help = 'password' , default = ''                  )

            # Separate your custom args from the standard Beam args.
            known_args, pipeline_args = parser.parse_known_args( None )

            self.set_postgres_config( known_args, pg_group )
            return known_args, pipeline_args

        except Exception as e:
            logging.error( 'Bat_03.get_args(), error: {}'.format( e ) )
            raise

    def __init__( self ):
        try:
            print( '' )
        except Exception as e:
            logging.error(  'Bat_03.__init__(), error: {}'.format( e )  )
            raise

if __name__ == '__main__':
    batch = Bat_03()
    batch.run()