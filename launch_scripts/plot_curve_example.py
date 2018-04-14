import sys, json, os
import pandas as pd
import matplotlib.pyplot as plt

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

            self.tp_stable_index, self.tp_stable = self._get_tp_stable(tp_df, 5, 0.1)
            self.latency_at_tp_stable_description = lat_df.head(self.tp_stable_index + 1)['value'].describe([0.25, 0.5, 0.75, 0.9])
            self.latency_at_tp_stable = self.latency_at_tp_stable_description['50%']

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


def load(fname):
    with open(fname) as fp:
        result = json.load(fp)

    parsed = ExperimentResult(result)

    return parsed

# -------------------- MAIN --------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide file name, please'
        sys.exit(1)

    file_name = sys.argv[1]
    parsed = load(file_name)

    lat_df = pd.DataFrame(parsed.latency_curve)
    lat_df = lat_df[lat_df.actualRate > 0].reset_index(drop=True).reset_index() #make the index a column
    tp_df = pd.DataFrame(parsed.throughput_curve)
    tp_df = tp_df[tp_df.actualRate > 0].reset_index(drop=True).reset_index() #make the index a column

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    tp = tp_df.plot(ax=ax1, kind='line', x='index', y='value', legend=False, style=['k-'])
    actual = tp_df.plot(ax=ax1, kind='line', x='index', y='actualRate', legend=False, style=['k:'])
    lat = lat_df.plot(ax=ax2, kind='line', x='index', y='value', legend=False, style=['k--'])

    ax1.set_ylim((0, ax1.yaxis.get_data_interval()[1]))
    ax1.set_xlim(xmin=30)
    ax1.margins(y=0.1)
    ax2.set_ylim((1, 500))
    ax2.set_xlim(xmin=30)
    ax2.margins(y=0.1)
    ax2.set_yscale('log')

    ax1.annotate(
        'sustainable throughput',
        xy=(parsed.tp_stable_index, parsed.tp_stable),
        xytext=(-20, 20),
        textcoords='offset points', ha='right', va='bottom',
        bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.5),
        arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0')
    )

    for a in (ax1, ax2):
        a.set_xlabel('')
        a.tick_params(axis=u'both', which=u'both',length=0)
        a.set_xticklabels([])
        a.set_yticklabels([])

    fig.savefig(file_name + '.png')
    plt.close(fig)
