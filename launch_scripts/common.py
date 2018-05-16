import pandas as pd
import traceback

class ExperimentResult(object):
    MIN_BATCHES = 10

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
            # the third batch if long enough
            batch_no = 2 if len(self.latency_curve) >= 3 else self.latency_curve[-1]
            self.latency_unloaded = self.latency_curve[batch_no]['value']
            self.tp_max = self._results['max-throughput-and-latency']['max-throughput']
            self.latency_at_tp_max = self._results['max-throughput-and-latency']['latency-at-max-throughput']

            if self.experiment_type == 'query':
                self.avg_partitions = self._results['number-of-partitions-per-query']['mean']

            lat_df = pd.DataFrame(self.latency_curve)
            lat_df = lat_df[lat_df.actualRate > 0].reset_index(drop=True)
            tp_df = pd.DataFrame(self.throughput_curve)
            tp_df = tp_df[tp_df.actualRate > 0].reset_index(drop=True)

            self.tp_stable_index, self.tp_stable = self._get_tp_stable(tp_df, 5, 0.1)
            self.latency_at_tp_stable_description = lat_df.head(tp_stable_index + 1)['value'].describe([0.25, 0.5, 0.75, 0.9])
            self.latency_at_tp_stable = self.latency_at_tp_stable_description['50%']
        except:
            self.valid = False
            self.problems = ['>> EXCEPTION WHILE PARSING <<']
            self.problems.extend(traceback.format_exc().split('\n'))

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

        if len(result['accumulators']['latency-curve']) < self.MIN_BATCHES:
            reasons.append(
                'Less then {} batches, please check.'.format(self.MIN_BATCHES))

        exceptions = result.get('exceptions')
        if  exceptions and len(exceptions['all-exceptions']) > 0:
            message = exceptions['root-exception']
            reasons.append('>> EXCEPTION IN JOB <<')
            reasons.extend(message.split('\n'))

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
            x = float(config['avg'])

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
