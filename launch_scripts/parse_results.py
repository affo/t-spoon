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

            lat_df = pd.DataFrame(self.latency_curve)
            lat_df = lat_df[lat_df.actualRate > 0].reset_index(drop=True)
            tp_df = pd.DataFrame(self.throughput_curve)
            tp_df = tp_df[tp_df.actualRate > 0].reset_index(drop=True)

            tp_stable_index, self.tp_stable = self._get_tp_stable(tp_df, 5, 0.1)
            self.latency_at_tp_stable_description = lat_df.head(tp_stable_index + 1)['value'].describe([0.25, 0.5, 0.75, 0.9])
            self.latency_at_tp_stable = self.latency_at_tp_stable_description['50%']
        except Exception as ex:
            self.valid = False
            self.problems = ['Malformed results: ' + str(ex)]

    def _get_tp_stable(self, tp_curve_df, w, t):
        map_fn = lambda row: float(row.actualRate - row.value) / row.actualRate
        error = tp_curve_df.apply(map_fn, axis=1)

        index = -1
        for i in error.index:
            next_w = error.loc[i:i+w] # the next w rows
            val = next_w.mean()
            if val > t:
                index = i
                break

        # if the condition wasn't met, then, we report the last one
        if index == -1:
            index = len(error) - 1

        return index, tp_curve_df.loc[index,:].value

    def _get_lat_stable(self, lat_curve_df, w, t):
        index = 0
        prev_window_mean = -1
        for i in lat_curve_df.index:
            window = lat_curve_df.loc[i:i+w,"value"] # the next w rows
            val = window.mean()
            error = float(val - prev_window_mean) / prev_window_mean
            if error > t:
                index = i
                break
            prev_window_mean = val

        return index, lat_curve_df.loc[index,:].value

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
            # TODO this should change with new query experiments...
            if not '1000' in label:
                x = float(config['queryPerc'])

        if 'keyspace' in label:
            experiment_type = 'ks'
            x = int(config['ks'])

        if experiment_type is None:
            if '1tg' in label:
                experiment_type = '1tg'
                x = int(config['noStates'])
            else:
                experiment_type = 'ntg'
                x = int(config['noTG'])

            series_or_parallel = 'series' if config['series'] == 'true' else 'parallel'

        tags = dict(
            label=label,
            strategy=strategy,
            isolation_level=isolation_level,
            experiment_type=experiment_type,
            x=x
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
    lat_des_columns = [
        'count', 'mean', 'std', 'min', '25%', '50%', '75%', '90%', 'max',
        'strategy', 'isolationLevel', 'var', 'tag1', 'tag2',
    ]
    throughput_df = []
    latency_df = []
    aggregates_df = []
    latency_description_df = []
    problems = []
    number_of_experiments = 0

    def extract_columns(result):
        param = result.x
        tag1 = result.experiment_type
        tag2 = None
        if tag1 not in ('query', 'ks'):
            tag2 = str(tag1)
            tag1 = result.series_or_parallel

        if tag1 == 'query' and param is None:
            tag2 = 'fixed'

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
        latency_description_df.append(list(result.latency_at_tp_stable_description) + tags)

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
    lat_des = pd.DataFrame(latency_description_df, columns=lat_des_columns)
    return tp, lat, aggr, lat_des, problems



# -------------------- MAIN --------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]

    tp, lat, aggr, lat_des, problems = load_results(folder_name)

    out_fname = os.path.join(folder_name, 'parsed')
    suffix = lambda s: '{}_{}.json'.format(out_fname, s)

    tp.to_json(suffix('throughput'))
    lat.to_json(suffix('latency'))
    lat_des.to_json(suffix('latency_description'))
    aggr.to_json(suffix('aggregates'))
    with open(suffix('problems'), 'w') as fp:
        json.dump(problems, fp, indent=4, sort_keys=True)

    print '>>> Output written to parsed_*.json'
    print '>>> Problems detected:', len(problems)
