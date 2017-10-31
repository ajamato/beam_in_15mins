import re
import apache_beam as beam

def basic_pipeline():
  print "Run basic_pipeline"
  # Create a pipeline executing on a direct runner (local, non-cloud).
  p = beam.Pipeline('DirectRunner')
  # Create a PCollection with names and write it to a file.
  (p
  | 'add names' >> beam.Create(['Ann', 'Joe'])
  | 'save' >> beam.io.WriteToText('./names'))
  # Execute the pipeline.
  p.run()

def basic_pipeline_with_map():
  print "Run basic_pipeline_with_map"
  p = beam.Pipeline('DirectRunner')
  # Read a file containing names, add a greeting to each name, and write to a file.
  (p
  | 'load namees' >> beam.io.ReadFromText('./names-00000-of-00001')  ## change to ./names
  | 'add greeting' >> beam.Map(lambda from_prev_transform: '%s' % (from_prev_transform))   
  | 'save' >> beam.io.WriteToText('./greetings'))
  p.run()

def basic_pipline_with_flat_map():
  print "Run basic_pipline_with_flat_map"
  p = beam.Pipeline('DirectRunner')
  # Read a file containing names, add two greetings to each name, and write to a file.
  (p
  | 'load names' >> beam.io.ReadFromText('./names-00000-of-00001')  ## Change to ./names
  | 'add greetings' >> beam.FlatMap(
          lambda name, messages: ['%s %s!' % (msg, name) for msg in messages],
          ['Hello', 'Hola']
      )
  | 'save' >> beam.io.WriteToText('./greetings'))

  p.run()

def basic_pipline_with_flat_map_and_yeild():
  print "Run basic_pipline_with_flat_map_and_yeild"
  p = beam.Pipeline('DirectRunner')
  # Read a file containing names, add two greetings to each name 
  # (with FlatMap using a yield generator), and write to a file.
  def add_greetings(name, messages):
    for msg in messages:
      yield '%s %s!' % (msg, name)

  (p
  | 'add names' >> beam.Create(['Ann', 'Joe'])
  | 'load names' >> beam.io.ReadFromText('./names-00000-of-00001')
  | 'greet' >> beam.FlatMap(add_greetings, ['Hello', 'Hola'])
  | 'save2' >> beam.io.WriteToText('./greetings'))
  p.run()

def counting_words():
  print "Run counting_words"
  p = beam.Pipeline('DirectRunner')
  (p
  | 'read' >> beam.io.ReadFromText('greetings-00000-of-00001')
  | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
  #| 'count words' >> beam.combiners.Count.PerElement() # TODO not working
  | 'save' >> beam.io.WriteToText('./word_count'))
  p.run()

# TODO not working
def counting_words_with_gbk():
  print "Run counting_words_with_gbk"
  p = beam.Pipeline('DirectRunner')
  class MyCountTransform(beam.PTransform):
    def apply(self, pcoll):
      return (pcoll
      | 'one word' >> beam.Map(lambda word: (word, 1))
      # GroupByKey accepts a PCollection of (word, 1) elements and
      # outputs a PCollection of (word, [1, 1, ...])
      | 'group words' >> beam.GroupByKey()
      | 'count words' >> beam.Map(lambda (word, counts): (word, len(counts))))

  (p
  | 'read' >> beam.io.ReadFromText('./names*')
  | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
  | MyCountTransform()
  | 'save' >> beam.io.WriteToText('./word_count'))
  p.run()

def combiner():
  print "Run combiner"
  p = beam.Pipeline('DirectRunner')
  SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20)]

  (p
  | beam.Create(SAMPLE_DATA)
  | beam.CombinePerKey(sum)
  | beam.io.WriteToText('./sums'))
  p.run()

basic_pipeline()
#basic_pipeline_with_map()
#basic_pipline_with_flat_map()
#basic_pipline_with_flat_map_and_yeild()
#counting_words()
#counting_words_with_gbk()
#combiner()