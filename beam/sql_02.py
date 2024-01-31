'''
This example read a list of strings,
count the number of times per each word,
using FlatMap and Combine
and write it to a txt file.

links:
    https://beam.apache.org/get-started/wordcount-example
    https://github.com/Beam-College/season-2022/blob/main/day2/5-PuttingAllTogether-Demo.ipynb
'''

import re
import apache_beam as beam
from apache_beam.options.pipeline_options   import PipelineOptions
from apache_beam.transforms.sql             import SqlTransform

output_file = '/home/art/data/tmp/sql_01.txt'

elements = [
     {'country': 'Norway'       , 'population':  5      , 'continent': 'America'    },
     {'country': 'Ghana'        , 'population': 31      , 'continent': 'Africa'     },
     {'country': 'Thailand'     , 'population': 70      , 'continent': 'Asia'       },
     {'country': 'Hungary'      , 'population': 10      , 'continent': 'Europe'     },
     {'country': 'Antartica'    , 'population':  0.005  , 'continent': 'Antartica'  },
     {'country': 'Fiji'         , 'population':  0.9    , 'continent': 'Oceania'    },
     {'country': 'Burkina Faso' , 'population': 20      , 'continent': 'Africa'     },
     {'country': 'Australia'    , 'population': 25      , 'continent': 'Oceania'    },
     {'country': 'South Korea'  , 'population': 52      , 'continent': 'Asia'       },
     {'country': 'Canada'       , 'population': 38      , 'continent': 'America'    }
]

query = """
        SELECT 
            continent, 
            SUM(population) total_population
        FROM 
            PCOLLECTION 
        GROUP BY 
            continent
        """

def format_row(row, header="Row"):
    fields = [header]
    for i, field in enumerate(row._fields):
        column = f"\n\t{field}: {row[i]}"
        fields.append(column)
    s = "".join(fields)
    print(s)
    return row

options = PipelineOptions(
    runner          = 'DirectRunner',
    project         = 'baywatch',
    job_name        = 'job_feed_sharks',
    temp_location   = '/home/art/data/tmp'
)


p = beam.Pipeline()


rows = (p | beam.Create(elements)
          | beam.Map(lambda x: beam.Row(country=str(x["country"]), # Need to specify type
                                    population=float(x["population"]),
                                    continent=str(x["continent"])))
)


sql = (rows | SqlTransform(query)
            | beam.Map(format_row)
)

p.run()



print( '\n End.' )


