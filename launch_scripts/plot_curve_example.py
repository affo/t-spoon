import sys, json, os
import pandas as pd
import matplotlib.pyplot as plt
from common import *


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
