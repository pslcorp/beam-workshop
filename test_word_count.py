import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from word_count import word_count_logic

class WordCountTest(unittest.TestCase):
  def test_count_words(self):
    lines_data = [
      'foo FOO',
      'Bar',
      'baz bAz BaZ'
    ]

    with TestPipeline() as pipeline:
      result = word_count_logic(lines = pipeline | beam.Create(lines_data))

      assert_that(
        result,
        equal_to([
          ('foo', 2),
          ('bar', 1),
          ('baz', 3)
        ])
      )
