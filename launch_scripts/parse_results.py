import sys, json, os
import pandas as pd

# the stability throughput is the throughput at the batch before the
# latency is above a specified threshold (e.g. 100ms)
#
# X: number of states / number of tgs / ks size
# Y: min-latency / stability throughput / maximum throughput
#
class ExperimentResult(object):
    MIN_BATCHES = 10
    LATENCY_STABILITY_THRESHOLD = 100 #ms

    def __init__(self, result_dict):
        try:
            self.valid = True
            self.problems = self._check_result(result_dict)

            self._raw = result_dict
            self._tags = self._parse_tags(
                result_dict['config']['execution-config']['user-config'])

            for tag, value in self._tags.iteritems():
                setattr(self, tag, value)

            self._results = self._raw['accumulators']
            self.latency_curve = self._results['latency-curve']
            self.throughput_curve = self._results['throughput-curve']

            self.latency_min = min([point['value'] for point in self.latency_curve])
            self.tp_max = self._results['max-throughput-and-latency']['max-throughput']
            self.latency_at_tp_max = self._results['max-throughput-and-latency']['latency-at-max-throughput']

            # find the stability point
            batch_number = -1
            for point in self.latency_curve:
                latency = point['value']
                if latency > self.LATENCY_STABILITY_THRESHOLD:
                    break;

                batch_number += 1

            self.tp_stable = self.throughput_curve[batch_number]['value']

            df = pd.DataFrame(self.latency_curve)
            self.latency_at_tp_stable = \
                df[df.value <= self.LATENCY_STABILITY_THRESHOLD]['value'].mean()
        except Exception as ex:
            self.valid = False
            self.problems = ['Malformed results: ' + str(ex)]


    def _check_result(self, result):
        reasons = []

        notes = result['additional-notes']
        if notes:
            reasons.append(notes)

        exceptions = result.get('exceptions')
        if  exceptions and len(exceptions['all-exceptions']) > 0:
            reasons.append('An exception was thrown.')

        if len(result['accumulators']['latency-curve']) < self.MIN_BATCHES:
            reasons.append(
                'Less then {} batches, please check.'.format(self.MIN_BATCHES))

        return None if len(reasons) == 0 else reasons


    def _parse_tags(self, config):
        label = config['label']
        strategy = 'OPT' if config['optOrNot'] == 'true' else 'PESS'
        isolation_level = 'PL' + str(config['isolationLevel'])

        experiment_type = None
        series_or_parallel = None
        x = None

        if 'query' in label:
            experiment_type = 'query'
            x = config['inputRate']

        if 'keyspace' in label:
            experiment_type = 'ks'
            x = config['ks']

        if experiment_type is None:
            if '1tg' in label:
                experiment_type = '1tg'
                x = config['noStates']
            else:
                experiment_type = 'ntg'
                x = config['noTG']

            series_or_parallel = 'series' if config['series'] == 'true' else 'parallel'

        tags = dict(
            label=label,
            strategy=strategy,
            isolation_level=isolation_level,
            experiment_type=experiment_type,
            x=int(x)
        )

        if series_or_parallel:
            tags['series_or_parallel'] = series_or_parallel

        return tags


    def __repr__(self):
        dictionary = {}
        dictionary.update(self._tags)
        dictionary.update(dict(
            latency_min=self.latency_min,
            tp_max=self.tp_max,
            latency_at_tp_max=self.latency_at_tp_max,
            tp_stable=self.tp_stable,
            latency_at_tp_stable=self.latency_at_tp_stable
        ))
        return str(dictionary)


def load_results(folder_name):
    curves_columns = [
        'inRate', 'value', 'strategy', 'isolationLevel', 'var',
        'tag1', 'tag2',
    ]
    aggregates_columns = [
        'value', 'strategy', 'isolationLevel', 'var',
        'tag1', 'tag2', 'tag3'
    ]
    throughput_df = []
    latency_df = []
    aggregates_df = []
    problems = []
    number_of_experiments = 0

    def extract_columns(result):
        param = result.x
        tag1 = result.experiment_type
        tag2 = None
        if tag1 not in ('query', 'ks'):
            tag2 = str(tag1)
            tag1 = result.series_or_parallel

        return [
            result.strategy,
            result.isolation_level,
            param, tag1, tag2
        ]

    def append(result):
        tags = extract_columns(result)

        aggregates_df.append([result.latency_at_tp_stable] + tags + ['lat_stable'])
        aggregates_df.append([result.tp_max] + tags + ['tp_max'])
        aggregates_df.append([result.tp_stable] + tags + ['tp_stable'])

        mappings = [
            (result.throughput_curve, throughput_df),
            (result.latency_curve, latency_df),
        ]
        for mapping in mappings:
            curve = mapping[0]
            frame = mapping[1]
            for point in curve:
                row = [
                    point['actualRate'],
                    point['value']
                ]
                row.extend(tags)
                frame.append(row)


    for subdir, dirs, files in os.walk(folder_name):
        for file in files:
            if not file.startswith('parsed') \
                and file.endswith('.json'):
                number_of_experiments += 1
                fname = os.path.join(subdir, file)
                print '>>> Parsing', fname

                result = None
                with open(fname) as fp:
                    result = json.load(fp)

                parsed = ExperimentResult(result)
                if parsed.problems != None:
                    problems.append(dict(label=fname, reasons=parsed.problems))

                if parsed.valid:
                    append(parsed)


    print '>>> Number of experiments:', number_of_experiments

    tp = pd.DataFrame(throughput_df, columns=curves_columns)
    lat = pd.DataFrame(latency_df, columns=curves_columns)
    aggr = pd.DataFrame(aggregates_df, columns=aggregates_columns)
    return tp, lat, aggr, problems



# -------------------- MAIN --------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]

    tp, lat, aggr, problems = load_results(folder_name)

    out_fname = os.path.join(folder_name, 'parsed')
    suffix = lambda s: '{}_{}.json'.format(out_fname, s)

    tp.to_json(suffix('throughput'))
    lat.to_json(suffix('latency'))
    aggr.to_json(suffix('aggregates'))
    with open(suffix('problems'), 'w') as fp:
        json.dump(problems, fp, indent=4, sort_keys=True)

    print '>>> Output written to parsed_*.json'
    print '>>> Problems detected:', len(problems)
