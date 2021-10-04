#!/usr/bin/env python3

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def word_count_logic(lines):
  return (lines
    | 'ExtractWords' >> beam.FlatMap(lambda line: line.lower().split(' '))
    | beam.Map(lambda word: (word, 1))
    | 'Word count' >> beam.CombinePerKey(sum)
  )

def word_count(beam_options, filename):
  with beam.Pipeline(options = beam_options) as pipeline:
    result = word_count_logic(
      lines = pipeline | 'Input data' >> beam.io.ReadFromText(filename)
    )

    (result
      | 'Format output' >> beam.MapTuple(lambda word, count: f'{word} : {count}')
      | 'Result' >> beam.Map(print)
    )

def main():
  beam_options = PipelineOptions(
    runner = 'DirectRunner'
  )
  word_count(beam_options, filename = 'data.txt')

if __name__ == '__main__':
  main()
