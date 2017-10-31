import apache_beam as beam
import debug
import csv


RECORDS = [
  ["2017-10-01", "CDN", "1380", "books"],
  ["2017-10-02", "Canada", "1717", "tv"],
  ["2017-10-02", "CANADA", "1259", "tv"],
]

print "Step 1: Write a function"
def normalize_country_code(element):
  COUNTRY_CODE_MAP = {
    "CANADA" : "CDN",
    "CDN" : "CDN",
    "UNITED STATES" : "USA",
    "USA" : "CDN",
    "UNITED STATES OF AMERICA" : "USA",
    "MEXICO": "MX",
    "MX": "MX"
  }
  element[1] = COUNTRY_CODE_MAP[element[1].upper()]
  return element

result = normalize_country_code(["2017-10-02", "Canada", "1717", "tv"])
print "result %s" % result

"""
def word_length(element):
  return len(element)

result = word_length("Beam")
print "result %s" % result
"""

print "Step 2: Write a parallel function"
# and run it with direct runner

class NormalizeCountryCodeFn(beam.DoFn):
  def process(self, element):
    return [normalize_country_code(element)]

p = beam.Pipeline('DirectRunner')
result = (p
| 'add sales records' >> beam.Create(RECORDS)
| 'run function in parallel' >> beam.ParDo(NormalizeCountryCodeFn()))

debug.print_pcoll(result)
p.run()

"""
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [word_length(element)]

function = ComputeWordLengthFn()
result = function.process("Beam")
print "result %s" % result

p = beam.Pipeline('DirectRunner')
result = (p
| 'add names' >> beam.Create(['Ann', 'Joe'])
#| 'run function in parallel' >> beam.Map(word_length))
| 'run function in parallel' >> beam.ParDo(ComputeWordLengthFn()))

debug.print_pcoll(result)
p.run()
"""

# Step 3
print "Step 3: Write a parallel function on words in a file"

class ParseCsvRow(beam.DoFn):
  def process(self, element):
    return [element.split(",")]

p = beam.Pipeline('DirectRunner')
result = (p
| 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')
| 'parse csv ' >> beam.ParDo(ParseCsvRow())
| 'run function in parallel ' >> beam.ParDo(NormalizeCountryCodeFn())
)
debug.print_pcoll(result)
p.run()

"""
p = beam.Pipeline('DirectRunner')
result = (p
| 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')

)
debug.print_pcoll(result)
"""

#| 'run function in parallel' >> beam.Map(word_length))
#| 'run function in parallel' >> beam.ParDo(ComputeWordLengthFn()))

#debug.print_pcoll(result)


# Step 4
print "Step 4: Modify the parallel function. Drop rows without a product id."

class NormalizeAndFilterRecordsFn(beam.DoFn):
  def process(self, element):
    element = normalize_country_code(element)
    if element[3]:
      return [element]
    return []

p = beam.Pipeline('DirectRunner')
result = (p
| 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')
| 'parse csv ' >> beam.ParDo(ParseCsvRow())
| 'run function in parallel ' >> beam.ParDo(NormalizeAndFilterRecordsFn())
)
debug.print_pcoll(result)
p.run()

# Step 5
print "Step 5: Group by the category ID and count sales."

class KeyByCountryCodeFn(beam.DoFn):
  def process(self, element):
    element = normalize_country_code(element)
    return [(element[1], element)]


p = beam.Pipeline('DirectRunner')
result = (p
| 'add names' >> beam.io.ReadFromText('./data/sample_sales_records.csv')
| 'parse csv ' >> beam.ParDo(ParseCsvRow())
| 'run function in parallel ' >> beam.ParDo(NormalizeAndFilterRecordsFn())
| 'key by country code' >> beam.ParDo(KeyByCountryCodeFn())
| 'group records' >> beam.GroupByKey()
| 'count records' >> beam.combiners.Count.PerElement()
)
debug.print_pcoll(result)
p.run()



