import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    def map_fn(strategy):
        return 'LB-' if strategy == 'PESS' else 'TB-'

    df = df[(df.isolationLevel != 'PL0') & (df.isolationLevel != 'PL1')]
    df['strategy'] = df['strategy'].map(map_fn) + df['isolationLevel']

    # ------ throughput
    cmn.reset_colors_and_markers()
    tp = df[(df.tag1 == 'ks') & (df.tag3 == 'tp')]
    tp = tp.sort_values('var')

    fig, ax = plt.subplots()
    for key, group in tp.groupby('strategy'):
        cmn.my_plot(group, ax, kind='line', x='var', y='value', label=key)

    ax.set_ylim((0, 15000))
    ax.margins(y=0.1)
    ax.set_ylabel('sustainable throughput [tr/s]')
    ax.set_xlabel('keyspace size')
    ax.set_xscale('log')
    plt.gca().invert_xaxis()
    cmn.savefig('ks_tp', fig)

    # ------ latency
    cmn.reset_colors_and_markers()
    lat = df[(df.tag1 == 'ks') & (df.tag3 == 'lat')]
    lat = lat.sort_values('var')

    fig, ax = plt.subplots()
    for key, group in lat.groupby('strategy'):
        cmn.my_plot(group, ax, kind='line', x='var', y='value', label=key)

    ax.set_ylim((0, 100))
    ax.margins(y=0.1)
    ax.set_ylabel('average latency [ms]')
    ax.set_xlabel('keyspace size')
    ax.set_xscale('log')
    plt.gca().invert_xaxis()
    cmn.savefig('ks_lat', fig)
