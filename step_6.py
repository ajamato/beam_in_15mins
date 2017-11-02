import apache_beam as beam
from apache_beam.typehints import typehints
import debug
import csv
import step_3
import step_4


class KeyByCountryAndCategoryFn(beam.DoFn):
  """A DoFn which accepts sales records and outputs key value tuples
  
  The output records are tuples containing the country code as a key
  and the original record as the value.
  """
  def process(self, element):
    return [(element[1] + "_" + element[3], element)]

if __name__ == "__main__":
  p = beam.Pipeline('DirectRunner')
  result = (p
  | 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')
  | 'parse csv ' >> beam.ParDo(step_3.ParseCsvRow())
  | 'run function in parallel ' >> beam.ParDo(
        step_4.NormalizeAndFilterRecordsFn())
  | 'key by country and category' >> beam.ParDo(KeyByCountryAndCategoryFn())
      .with_output_types(typehints.KV[str, typehints.List[str]])
  | 'count records' >>beam.combiners.Count.PerKey()
  )

  result | 'write to file' >> beam.io.WriteToText('output/results')
  debug.print_pcoll(result)
  p.run()
  print "You can also find this output in output/results*"

# TODO fix this file, it is not working