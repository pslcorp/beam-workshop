#!/usr/bin/env python3

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from dataclasses import dataclass

def basic_pipeline(beam_options):
  with beam.Pipeline(options = beam_options) as pipeline:
    (pipeline
      | 'Input data' >> beam.Create([1, 2, 3, 4])
      | 'Duplicate' >> beam.Map(lambda x: x * 2)
      | 'Sum everything' >> beam.CombineGlobally(sum)
      | 'Result:' >> beam.Map(print)
    )

@dataclass
class Apartment:
  name: str
  area: float
  price: float

def calculated_values(beam_options):
  with beam.Pipeline(options = beam_options) as pipeline:
    apartments_data = pipeline | beam.Create([
      Apartment('Biocity', 60.44, 368.60),
      Apartment('Vierzo', 102.32, 553.03),
      Apartment('Punta del Parque', 103.61, 509.62),
      Apartment('Cibeles', 61.47, 268.18),
      Apartment('EPIC', 60.41, 565.90),
      Apartment('Senior\'s Club - El Vergel', 53.49, 389.68),
      Apartment('Alcala de Henares', 79.75, 277.72),
      Apartment('PradoAlto', 97.5, 502.00)
    ])

    apartments_price_per_square_meter = apartments_data | beam.Map(
      lambda apartment: (apartment.name, (apartment.price / apartment.area))
    )

    square_meter_price_mean = (apartments_price_per_square_meter
      | beam.Values()
      | beam.combiners.Mean.Globally()
    )

    apartments_price_per_square_meter_bellow_mean = apartments_price_per_square_meter | beam.Filter(
      lambda apartment_price_per_square_meter, average_square_meter_price: apartment_price_per_square_meter[1] <= average_square_meter_price,
      average_square_meter_price = beam.pvalue.AsSingleton(square_meter_price_mean)
    )

    apartments_price_per_square_meter_bellow_mean | beam.Map(print)

def main():
  beam_options = PipelineOptions(
    runner = 'DirectRunner'
  )
  basic_pipeline(beam_options)
  print('--------------')
  calculated_values(beam_options)

if __name__ == '__main__':
  main()
