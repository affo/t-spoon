import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt

# ------------ MAIN ------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]
    img_folder = os.path.join(folder_name, 'img')

    out_fname = os.path.join(folder_name, 'parsed')
    tp = pd.read_json(out_fname + '_throughput.json')
    lat = pd.read_json(out_fname + '_latency.json')
    lat_des = pd.read_json(out_fname + '_latency_description.json')
    aggr = pd.read_json(out_fname + '_aggregates.json')

    def savefig(label, figure):
        figure.savefig(os.path.join(img_folder, label + '.png'))
        plt.close(figure)

    def map_fn(strategy):
        return 'LB-' if strategy == 'PESS' else 'TB-'

    aggr = aggr[(aggr.isolationLevel != 'PL0') & (aggr.isolationLevel != 'PL1')]
    aggr['strategy'] = aggr['strategy'].map(map_fn) + aggr['isolationLevel']

    # ------ throughput
    tp = aggr[(aggr.tag1 == 'ks') & (aggr.tag3 == 'tp_stable')]
    tp = tp.sort_values('var')

    fig, ax = plt.subplots()
    for key, group in tp.groupby('strategy'):
        group.plot(ax=ax, kind='line', x='var', y='value', label=key)

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('sustainable throughput [tr/s]')
    ax.set_xlabel(' ')
    ax.set_xscale('log')
    plt.gca().invert_xaxis()
    savefig('ks_tp', fig)

    # ------ latency
    lat = aggr[(aggr.tag1 == 'ks') & (aggr.tag3 == 'lat_stable')]
    lat = lat.sort_values('var')

    fig, ax = plt.subplots()
    for key, group in lat.groupby('strategy'):
        group.plot(ax=ax, kind='line', x='var', y='value', label=key)

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('average latency [ms]')
    ax.set_xlabel(' ')
    ax.set_xscale('log')
    plt.gca().invert_xaxis()
    savefig('ks_lat', fig)
