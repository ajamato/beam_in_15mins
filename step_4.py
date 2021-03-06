import apache_beam as beam
from apache_beam.typehints import typehints
import debug
import csv
import step_1
import step_2
import step_3

class NormalizeAndFilterRecordsFn(beam.DoFn):
  """A DoFn which normalizes and filters sales record elements.
  
  Normalizes country codes so that they are consistent across records.
     e.g. 'United States', 'USA', and 'United States of America' are
     all normalized to 'USA'
  Filters out the sales records with missing product ids.
  """
  def process(self, element):
    element = step_1.normalize_country_code(element)
    if element[2]:
      # Accept only rows with the second field set, which
      # represents the product id.
      return [element]
    return []

if __name__ == "__main__":
  p = beam.Pipeline('DirectRunner')
  result = (p
  | 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')
  | 'parse csv ' >> beam.ParDo(step_3.ParseCsvRow())
  | 'run function in parallel ' >> beam.ParDo(NormalizeAndFilterRecordsFn()))

  result | 'write to file' >> beam.io.WriteToText('output/results')
  debug.print_pcoll(result)
  p.run()
  print "You can also find this output in output/results*"