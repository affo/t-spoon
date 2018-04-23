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
    ks_size = 100000

    out_fname = os.path.join(folder_name, 'parsed')
    tp = pd.read_json(out_fname + '_throughput.json')
    lat = pd.read_json(out_fname + '_latency.json')
    lat_des = pd.read_json(out_fname + '_latency_description.json')
    aggr = pd.read_json(out_fname + '_aggregates.json')

    def savefig(label, figure):
        figure.savefig(os.path.join(img_folder, label + '.png'))
        plt.close(figure)

    def map_fn(strategy):
        return 'LB' if strategy == 'PESS' else 'TB'

    aggr = aggr[(aggr.tag1 == 'ks') & (aggr['var'] == ks_size)]
    aggr = aggr[(aggr.isolationLevel != 'PL0') & (aggr.isolationLevel != 'PL1')]
    aggr['strategy'] = aggr['strategy'].map(map_fn)

    lb = aggr[(aggr.strategy == 'LB')]
    tb = aggr[(aggr.strategy == 'TB')]

    def apply_fn(row):
        tplat = row.tag3
        isolation = row.isolationLevel
        value = row.value

        return pd.Series([tplat, isolation, value])

    lb = lb.apply(apply_fn, axis=1)
    lb.columns = ['tplat', 'isolation', 'LB']
    lb['key'] = lb['tplat'] + lb['isolation']
    tb = tb.apply(apply_fn, axis=1)
    tb.columns = ['tplat', 'isolation', 'TB']
    tb['key'] = tb['tplat'] + tb['isolation']
    ys = ['TB', 'LB']

    def merge_fn(row):
        tplat = row.tplat_lb if not pd.isna(row.tplat_lb) else row.tplat_tb
        isolation = row.isolation_lb if not pd.isna(row.isolation_lb) else row.isolation_tb
        tb = row.TB
        lb = row.LB
        return pd.Series([tplat, isolation, tb, lb])

    joined = lb.set_index('key').join(tb.set_index('key'), how='outer', lsuffix='_lb', rsuffix='_tb')
    joined = joined.apply(merge_fn, axis=1)
    joined.columns = ['tplat', 'isolation', 'TB', 'LB']

    # ------ throughput
    tp = joined[(joined.tplat == 'tp_stable')]

    fig, ax = plt.subplots()
    tp.plot(ax=ax, kind='bar', x='isolation', y=ys, rot=0)

    ax.set_ylim((0, 8000))
    ax.margins(y=0.1)
    ax.set_ylabel('sustainable throughput [tr/s]')
    ax.set_xlabel(' ')
    savefig('bank_isol_tp', fig)

    # ------ latency
    lat = joined[(joined.tplat == 'lat_unloaded')]

    fig, ax = plt.subplots()
    lat.plot(ax=ax, kind='bar', x='isolation', y=ys, rot=0)

    ax.set_ylim((0, 20))
    ax.margins(y=0.1)
    ax.set_ylabel('average latency [ms]')
    ax.set_xlabel(' ')
    savefig('bank_isol_lat', fig)
