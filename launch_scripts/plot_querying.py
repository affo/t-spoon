import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    # ------ throughput
    cmn.reset_colors_and_markers()
    tp = df[(df.tag1 == 'query') & (df.tag3 == 'tp')]
    tp = tp.sort_values('var')
    partitions = df[(df.tag1 == 'query') & (df.tag3 == 'avg_partitions')]
    partitions = partitions.sort_values('var')

    color1 = cmn.COLORS[0]
    marker1 = cmn.MARKERS[0]
    color2 = cmn.COLORS[1]
    marker2 = cmn.MARKERS[1]

    fig, ax = plt.subplots()
    ln_tp, = ax.plot(tp['var'].tolist(), tp['value'].tolist(),
        color=color1, marker=marker1, markerfacecolor='none')

    cmn.set_yaxis_formatter(ax)
    ax.set_ylim((0, 60000))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.TP_LABEL)
    ax.set_xlabel('Number of keys per query')
    ax.set_xscale('log')

    ax2 = ax.twinx()
    ln_part, = ax2.plot(partitions['var'].tolist(), partitions['value'].tolist(),
        color=color2, marker=marker2, markerfacecolor='none')
    ax2.set_ylabel('Average number of partitions')

    lns = (ln_tp, ln_part)
    labs = ('throughput', 'partitions')
    ax.legend(lns, labs, loc='upper center')
    cmn.savefig('query_tp', fig)

    # ------ latency
    cmn.reset_colors_and_markers()
    lat = df[(df.tag1 == 'query') & (df.tag3 == 'lat')]
    lat = lat.sort_values('var')

    fig, ax = plt.subplots()
    lat.plot(ax=ax, kind='line', x='var', y='value',
        color=color1, marker=marker1, markerfacecolor='none')

    ax.set_ylim((0, 100))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.LAT_LABEL)
    ax.set_xlabel('Number of keys per query')
    ax.set_xscale('log')

    cmn.savefig('query_lat', fig)
