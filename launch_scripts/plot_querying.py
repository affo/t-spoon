import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    # ------ throughput
    tp = df[(df.tag1 == 'query') & (df.tag3 == 'tp')]
    tp = tp.sort_values('var')
    partitions = df[(df.tag1 == 'query') & (df.tag3 == 'avg_partitions')]
    partitions = partitions.sort_values('var')

    fig, ax = plt.subplots()
    ln_tp, = ax.plot(tp['var'].tolist(), tp['value'].tolist(), '-')

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('sustainable throughput [tr/s]')
    ax.set_xlabel('number of keys per query')
    ax.set_xscale('log')

    ax2 = ax.twinx()
    ln_part, = ax2.plot(partitions['var'].tolist(), partitions['value'].tolist(), '-', color='r')
    ax2.set_ylabel('average number of partitions')

    lns = (ln_tp, ln_part)
    labs = ('throughput', 'partitions')
    ax.legend(lns, labs, loc='upper center')
    #plt.gca().invert_xaxis()
    cmn.savefig('query_tp', fig)

    # ------ latency
    lat = df[(df.tag1 == 'query') & (df.tag3 == 'lat')]
    lat = lat.sort_values('var')

    fig, ax = plt.subplots()
    lat.plot(ax=ax, kind='line', x='var', y='value')

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('average latency [ms]')
    ax.set_xlabel('number of keys per query')
    ax.set_xscale('log')
    #plt.gca().invert_xaxis()
    cmn.savefig('query_lat', fig)
