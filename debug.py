import apache_beam as beam

def print_element(element):
  print element

def print_pcoll(pcoll):
  """Creates a map step to prints every element in a PCollection"""
  pcoll | "Print PCollection:" >> beam.Map(print_element)
