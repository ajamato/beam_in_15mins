import apache_beam as beam
from apache_beam.typehints import typehints
import debug
import csv
import step_1
import step_2
import step_3

# Step 4
class NormalizeAndFilterRecordsFn(beam.DoFn):
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
  | 'run function in parallel ' >> beam.ParDo(NormalizeAndFilterRecordsFn())
  | 'write to file' >> beam.io.WriteToText('output/results')
  )
  debug.print_pcoll(result)
  p.run()
  print "You can also find this output in output/results*"