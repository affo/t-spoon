import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import container
from matplotlib.figure import figaspect
from matplotlib.ticker import FuncFormatter

import pandas as pd
import os, traceback, json, itertools

IMG_FOLDER = None
TP_LABEL = 'Sustainable throughput [Kel/s]'
LAT_LABEL = 'Average latency [ms]'

############## Set style for matplotlib
### Font
font = {'family': 'Times New Roman', 'size': 22}
mpl.rc('font', **font)

### Figure dimensions
_hw_ratio = 1.0 / 2.0
_width, _height = figaspect(_hw_ratio)

### Colors and markers
COLORS = ['black', 'darkorange', 'dodgerblue', 'crimson', 'forestgreen']
MARKERS = ['o', 'x', 'v', 's', 'p']

_it_colors = None
_it_markers = None

def reset_colors_and_markers():
    global _it_colors, _it_markers
    _it_colors = itertools.cycle(COLORS)
    _it_markers = itertools.cycle(MARKERS)

# reset at first import
reset_colors_and_markers()

##########################################################

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


def thousands(x, pos):
    'The two args are the value and tick position'
    if x < 1000:
        return str(x)

    return '%1.1f' % (x * 1e-3)

_formatter = FuncFormatter(thousands)

def set_yaxis_formatter(ax):
    ax.yaxis.set_major_formatter(_formatter)

def my_plot(data, ax, **kwargs):
    if kwargs.pop('yformatter', True):
        set_yaxis_formatter(ax)

    color = _it_colors.next()
    data.plot(
        ax=ax,
        color=color,
        marker=_it_markers.next(),
        markerfacecolor='none',
        **kwargs)

    if 'std' in data.columns:
        plt.errorbar(data['var'], data['value'], yerr=data['std'],
        linestyle='None', color=color, label=None, capsize=5)

    # redraw the legend for known bugs: https://github.com/pandas-dev/pandas/issues/14958
    # in case there is no external parameter
    if kwargs.get('legend') is None:
        handles, labels = ax.get_legend_handles_labels()
        handles = [h[0] if isinstance(h, container.ErrorbarContainer) else h for h in handles]
        ax.legend(handles, labels)


def savefig(label, figure):
    import os
    global IMG_FOLDER

    # change figure dimensions
    figure.set_figheight(_height)
    figure.set_figwidth(_width)

    # make the dir if not present
    if IMG_FOLDER is not None and not os.path.exists(IMG_FOLDER):
        os.mkdir(IMG_FOLDER)

    if IMG_FOLDER is None:
        IMG_FOLDER = os.getcwd()

    fname = os.path.join(IMG_FOLDER, label + '.pdf')
    figure.savefig(fname, bbox_inches='tight')
    plt.close(figure)
    print ">>> Figure saved to", fname


def savefig_with_separate_legend(label, ax, figure):
    global IMG_FOLDER

    handles, labels = ax.get_legend_handles_labels()
    # remove the legend from main figure
    ax.get_legend().set_visible(False)

    # saving the main figure
    savefig(label, figure)

    figlegend = plt.figure(figsize=(12, 0.7))
    figlegend.legend(handles, labels, loc=10, ncol=len(labels))
    fname = os.path.join(IMG_FOLDER, 'legendfor_' + label + '.pdf')
    figlegend.savefig(fname)
    plt.close(figlegend)
    print ">>> Legend saved to", fname


class ExperimentResult(object):
    def __init__(self, result_dict):
        try:
            self.valid = True
            self.problems = self._check_result(result_dict)

            self._raw = result_dict
            self._parallelism = int(result_dict['config']['execution-config']['job-parallelism'])
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

        if 'scale' in label:
            experiment_type = 'scale'
            x = self._parallelism

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
