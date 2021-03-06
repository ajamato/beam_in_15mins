import apache_beam as beam
from apache_beam.typehints import typehints
import debug
import csv
import step_1

class NormalizeCountryCodeFn(beam.DoFn):
  """A DoFn which normalizes the country code on sales records.
  
  When this is invoked in a beam pipeline it will normalize many
  sales record elements at the same time. That is, in parallel.
  """
  def process(self, element):
    return [step_1.normalize_country_code(element)]

RECORDS = [
  ["2017-10-01", "CDN", "1380", "books"],
  ["2017-10-02", "Canada", "1717", "tv"],
  ["2017-10-02", "CANADA", "1259", "tv"],
]

if __name__ == "__main__":
  p = beam.Pipeline('DirectRunner')
  result = (p
  | 'add sales records' >> beam.Create(RECORDS)
          .with_output_types(typehints.List[str])
  | 'run function in parallel' >> beam.ParDo(NormalizeCountryCodeFn()))

  debug.print_pcoll(result)
  p.run()