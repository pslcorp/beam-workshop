#!/usr/bin/env python3

import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    return 0.0, 0 # sum, count

  def add_input(self, accumulator, input):
    sum, count = accumulator
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    total_sum = 0.0
    total_count = 0

    for sum, count in accumulators:
      total_sum += sum
      total_count += count

    return total_sum, total_count

  def extract_output(self, accumulator):
    sum, count = accumulator
    return sum / count

def main():
  beam_options = PipelineOptions(
    runner = 'PortableRunner',
    job_endpoint = 'localhost:8099',
    environment_type = 'LOOPBACK'
  )
  with beam.Pipeline(options = beam_options) as pipeline:
    nba_raw_data = pipeline | beam.Create([
      '{"teamName":"ChicagoBulls","players":[{"playerName":"DwyaneWade","height":1.93,"weight":100.00},{"playerName":"BobbyPortis","height":2.11,"weight":111.58},{"playerName":"AnthonyMorrow","height":1.96,"weight":95.25},{"playerName":"DavidNwaba","height":1.93,"weight":94.80},{"playerName":"KrisDunn","height":1.93,"weight":95.25}]}',
      '{"teamName":"ClevelandCavaliers","players":[{"playerName":"LeBronJames","height":2.03,"weight":113.40},{"playerName":"JeffGreen","height":2.06,"weight":106.59},{"playerName":"KayFelder","height":1.75,"weight":79.83},{"playerName":"ChanningFrye","height":2.11,"weight":115.67},{"playerName":"KyrieIrving","height":1.90,"weight":87.54}]}',
      '{"teamName":"DetroitPistons","players":[{"playerName":"IshSmith","height":1.83,"weight":19.38},{"playerName":"ReggieBullock","height":2.00,"weight":92.99},{"playerName":"StanleyJohnson","height":2.00,"weight":111.13},{"playerName":"EricMoreland","height":2.10,"weight":107.96},{"playerName":"TobiasHarris","height":2.06,"weight":106.59}]}',
      '{"teamName":"LosAngelesLakers","players":[{"playerName":"LonzoBall","height":1.98,"weight":86.18},{"playerName":"ThomasBryant","height":2.10,"weight":112.49},{"playerName":"JordanClarkson","height":1.96,"weight":88.00},{"playerName":"LoulDeng","height":2.06,"weight":99.79},{"playerName":"ThomasRobinson","height":2.10,"weight":107.50}]}',
      '{"teamName":"NewYorkKnicks","players":[{"playerName":"CarmeloAnthony","height":2.03,"weight":108.86},{"playerName":"DamyeanDotson","height":1.96,"weight":95.25},{"playerName":"JoakimNoah","height":2.11,"weight":104.33},{"playerName":"FrankNtilikina","height":1.96,"weight":86.18},{"playerName":"RamonSessions","height":1.90,"weight":86.18}]}',
    ])
    nba_data = nba_raw_data | beam.Map(lambda json_str: json.loads(json_str))

    def extract_team_players_bmi(nba_team):
      def bmi(height, weight):
        return weight / (height ** 2)

      team_name = nba_team['teamName']
      return [
        (team_name, bmi(height = team_player['height'], weight = team_player['weight']))
        for team_player in nba_team['players']
      ]
    players_bmi = nba_data | beam.FlatMap(extract_team_players_bmi)

    teams_average_bmi = players_bmi | beam.CombinePerKey(AverageFn())
    teams_average_bmi | beam.Map(print)

if __name__ == '__main__':
  main()
