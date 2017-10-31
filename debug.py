import apache_beam as beam

def print_element(element):
  print element

def print_pcoll(pcoll):
  pcoll | "Print PCollection:" >> beam.Map(print_element)
