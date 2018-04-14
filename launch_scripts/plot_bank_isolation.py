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
    ks_size = 1000

    out_fname = os.path.join(folder_name, 'parsed')
    tp = pd.read_json(out_fname + '_throughput.json')
    lat = pd.read_json(out_fname + '_latency.json')
    lat_des = pd.read_json(out_fname + '_latency_description.json')
    aggr = pd.read_json(out_fname + '_aggregates.json')

    def savefig(label, figure):
        figure.savefig(os.path.join(img_folder, label + '.png'))
        plt.close(figure)


    def map_fn(strategy):
        return 'LB-' if strategy == 'PESS' else 'TO-'

    aggr['x'] = aggr['strategy'].map(map_fn) + aggr['isolationLevel']

    # ------ throughput
    tp = aggr[(aggr.tag1 == 'ks') & (aggr.tag3 == 'tp_stable')]
    tp['ks_size'] = tp['var'].map(int)
    tp = tp[tp.ks_size == ks_size]
    tp = tp.sort_values('x')

    fig, ax = plt.subplots()
    tp.plot(ax=ax, kind='bar', x='x', y='value', legend=False, rot=0)

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('sustainable throughput [tr/s]')
    ax.set_xlabel(' ')
    savefig('bank_isol_tp', fig)

    # ------ latency
    lat = aggr[(aggr.tag1 == 'ks') & (aggr.tag3 == 'lat_stable')]
    lat['ks_size'] = lat['var'].map(int)
    lat = lat[lat.ks_size == ks_size]
    lat = lat.sort_values('x')

    fig, ax = plt.subplots()
    lat.plot(ax=ax, kind='bar', x='x', y='value', legend=False, rot=0)

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('average latency [ms]')
    ax.set_xlabel(' ')
    savefig('bank_isol_lat', fig)

    # ax1.set_xlim(xmin=30)
    # ax1.margins(y=0.1)
    # ax2.set_ylim((1, 500))
    # ax2.set_xlim(xmin=30)
    # ax2.margins(y=0.1)
    # ax2.set_yscale('log')
