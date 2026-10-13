

import apache_beam as __beam__  # Required internal mapping for Interactive Beam
import apache_beam as beam
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

# 1. Initialize the Interactive Runner
runner = InteractiveRunner()
pipeline = beam.Pipeline(runner=runner)

# 2. Build the Pipeline
csv_data = (
    pipeline
    | "Read CSV" >> beam.io.ReadFromText("sample.csv", skip_header_lines=1)
    | "Parse Rows" >> beam.Map(lambda line: line.split(","))
    | "Filter Electronics" >> beam.Filter(lambda row: row[1] == "Electronics")
)

# 3. Visualize the Graph Pipeline (Crucial for your notes!)
# This renders a visual DAG diagram right inside your notebook
ib.show_graph(pipeline)

# 4. Inspect Data Mid-Pipeline without saving to an external file
# This brings up an interactive data table of your results
ib.show(csv_data)



'''
3. Key Concepts for Your NotesDirectRunner vs InteractiveRunner: While DirectRunner is the default for local testing via standard .py scripts, InteractiveRunner is specifically designed for prototyping, debugging, and visualization inside notebooks.ib.show_graph(pipeline): This maps your pipeline steps into an easy-to-read flowchart. It automatically labels the steps using the string descriptors you provided (e.g., "Read CSV", "Parse Rows").ib.show(pcollection): Instead of writing to a text file and opening it manually, this command lets you preview your data flowing through any specific step of the pipeline.
'''