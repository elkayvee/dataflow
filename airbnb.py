import os
import re
import sys
import logging
import csv
import pandas as pd

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from google.cloud.bigquery import SchemaField

schema_file = 'airbnb.json'

class DataTransformation:
    def __init__(self):
        self.schema_str = ''
        with open(schema_file) as ff:
            filedata = ff.read()
            self.schema_str = '{"fields": ' + filedata + '}'
        
    def parse_line(self, element):
        import re
        import csv
        from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

        schema = parse_table_schema_from_json(self.schema_str)
        field_map = [f for f in schema.fields]

        for line in csv.reader(element.split('\n'), quotechar='"', delimiter='\t',quoting=csv.QUOTE_ALL,skipinitialspace=True):
            row = {}
            i = 0
            values = [v for v in line]
            for value in values:
                row[field_map[i].name.encode('utf-8')] = value
                i += 1
        return row

    def count_listings(self, input_data):
        return (input_data
                | 'neighbourhood listings' >> beam.FlatMap(lambda row: [(row['neighbourhood'], 1)] if row['id'] else [])
                | 'listings count' >> beam.CombinePerKey(sum)
                | 'format' >> beam.Map(lambda k_v: {'neighbourhood': k_v[0], 'count': k_v[1]}))

    # Count the occurrences of each neighbourhood.
    def count_ones(self,neighbourhood_ones):
        (neighbourhood, ones) = neighbourhood_ones
        return (neighbourhood, sum(ones))

    # Format the counts into a PCollection of strings.
    def format_result(self,neighbourhood_count):
        (neighbourhood, count) = neighbourhood_count
        #return '%s: %d' % (neighbourhood, count)
        return {'neighbourhood': neighbourhood, 'count': count}

class CollectNeighbourhoods(beam.DoFn):
    def process(self, element):
        """
        Returns a list of tuples containing listing of 'neighbourhood' and 'id'
        """
        return [(element['neighbourhood'], element['id'])]

def run():
    """
    Triggers the execution of the pipeline
    """
    import apache_beam as beam
    from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
    import csv
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.options.pipeline_options import SetupOptions
    from apache_beam.options.pipeline_options import GoogleCloudOptions
    from apache_beam.options.pipeline_options import StandardOptions
    from apache_beam.io.textio import ReadFromText, WriteToText
    from google.cloud.bigquery import SchemaField


    PROJECT = 'w266-final-199221'
    BUCKET = 'springml-airbnb'

    input_filename  = 'gs://{0}/AB_NYC_2019.txt'.format(BUCKET) # sys.argv[1]
    #schema_file = 'gs://{0}/airbnb.json'.format(BUCKET)

    dataflow_options = ['--project={0}'.format(PROJECT),
                        '--job_name=springml-airbnb-job',
                        '--save_main_session',
                        '--temp_location=gs://{0}/temp/'.format(BUCKET),]
    dataflow_options.append('--staging_location=gs://{0}/stage/'.format(BUCKET))

    options = PipelineOptions(dataflow_options)
    gcloud_options = options.view_as(GoogleCloudOptions)

    # Dataflow runner
    options.view_as(StandardOptions).runner ='dataflow'
      
    data_ingestion = DataTransformation()

    # Read the schema of the BigQuery table from the JSON schema file generated by
    # `bq schema --format=prettyjson 'w266-final-199221':springml.airbnb | jq '.schema.fields' > schema_file.json
    # and uploaded to the gcs: 'gs://{0}/airbnb.json'.format(BUCKET)

    schema = parse_table_schema_from_json(data_ingestion.schema_str)

    # Set up the pipeline for Pcollections, p
    p = beam.Pipeline(options=options)

    # Read the text file[pattern] into a PCollection.
    rows=p|'Read from AB_NYC_2019.txt:' >> beam.io.ReadFromText(input_filename,skip_header_lines=1)

    # Parse the lines into constituent fields
    parsed_rows=(rows | 'Parse Original file to BigQuery Table Row:' >> beam.Map(lambda s:data_ingestion.parse_line(s)))

    # Save the original file into BigQuery table: project ID: 'w266-final-199221' dataset ID: springml table: 'airbnb'
    _=(parsed_rows|'Save original file to BigQuery table:' >> beam.io.Write(beam.io.BigQuerySink(
            'w266-final-199221:springml.airbnb',
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
       
    # Count the listings in each neighbourhood by using 'GroupByKey()'
    counts=(parsed_rows| 
            'Collect Neighbourhood Listings:' >> beam.ParDo(CollectNeighbourhoods()) |  
            'GroupBy neighbourhood Key:' >> beam.GroupByKey()|
            'Count neighbourhood listings:' >> beam.CombineValues(beam.combiners.CountCombineFn()) |
            'Format into Dictionary:' >> beam.Map(lambda s: data_ingestion.format_result(s))|
            'Save Counts to BigQuery table:' >> beam.io.WriteToBigQuery(
            'w266-final-199221:springml.airbnbsummary',
            schema='neighbourhood:STRING, count:INTEGER',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    result=p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
