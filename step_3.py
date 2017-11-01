import apache_beam as beam
from apache_beam.typehints import typehints
import debug
import csv
import step_1
import step_2

class ParseCsvRow(beam.DoFn):
  def process(self, element):
    # TODO add comments
    return [element.split(",")]

if __name__ == "__main__":
  p = beam.Pipeline('DirectRunner')
  result = (p
  | 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')
  | 'parse csv ' >> beam.ParDo(ParseCsvRow())
  | 'run function in parallel ' >> beam.ParDo(step_2.NormalizeCountryCodeFn())
  | 'write to file' >> beam.io.WriteToText('output/results')
  )
  debug.print_pcoll(result)
  p.run()
  print "You can also find this output in output/results*"