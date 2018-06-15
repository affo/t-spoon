import pandas as pd
import traceback, json

IMG_FOLDER = None

def load_experiment(fname):
    with open(fname) as fp:
        result = json.load(fp)

    parsed = ExperimentResult(result)
    return parsed


def load_parsed_results():
    import sys, os
    global IMG_FOLDER

    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]
    IMG_FOLDER = os.path.join(folder_name, 'img')

    out_fname = os.path.join(folder_name, 'parsed_results.json')
    out_fname2 = os.path.join(folder_name, 'aggregated.json')

    try:
        df = pd.read_json(out_fname)
    except:
        df = pd.read_json(out_fname2)

    return df


def savefig(label, figure):
    import os
    import matplotlib.pyplot as plt
    global IMG_FOLDER

    # make the dir if not present
    if not os.path.exists(IMG_FOLDER):
        os.mkdir(IMG_FOLDER)
    fname = os.path.join(IMG_FOLDER, label + '.png')
    figure.savefig(fname)
    plt.close(figure)
    print ">>> Figure saved to", fname


class ExperimentResult(object):
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

            lat = self._results.get('latency-unloaded')
            tp = self._results.get('sustainable-workload')

            self.latency = None if lat is None else lat.get('mean')
            self.throughput = None if tp is None else tp.get('mean')
            self.deviation = None

            if self.throughput is not None:
                self.deviation = tp.get('stddev') / self.throughput

            if self.experiment_type == 'query':
                self.avg_partitions = self._results.get('number-of-partitions-per-query', dict()).get('mean')
        except:
            self.valid = False
            self.problems = ['>> EXCEPTION WHILE PARSING <<']
            self.problems.extend(traceback.format_exc().split('\n'))

    def _check_result(self, result):
        reasons = []
        notes = result.get('additional-notes')
        if notes:
            reasons.append(notes)

        avg_curve = result['accumulators']['targeting-curve']
        if len(avg_curve) == 0:
            reasons.append('The job terminated prematurely.')

        exceptions = result.get('exceptions')
        if exceptions is not None and len(exceptions['all-exceptions']) > 0:
            message = exceptions['root-exception']
            reasons.append('>> EXCEPTION IN JOB <<')
            reasons.extend(message.split('\n'))

        return reasons

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
        if self.valid:
            dictionary.update(self._tags)
            dictionary.update(dict(
                latency=self.latency,
                throughput=self.throughput,
            ))
        return str(dictionary)
