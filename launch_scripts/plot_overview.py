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

    def fix_columns(row):
        chain = 'CHAIN' if row.tag1 == 'series' else 'PARALLEL'
        single = 'SINGLE' if row.tag2 == '1tg' else 'MULTI'
        tag = chain + '-' + single
        strategy = 'LB-' if row.strategy == 'PESS' else 'TB-'
        strategy += row.isolationLevel
        s = pd.Series({
            'value': row.value,
            'strategy': strategy,
            'var': row['var'],
            'type': tag,
            'tplat': row.tag3
        })
        return s

    aggr = aggr[(aggr.isolationLevel != 'PL0') & (aggr.isolationLevel != 'PL1')]
    aggr = aggr[(aggr.tag1 != 'query') & (aggr.tag1 != 'ks')]
    aggr = aggr.apply(fix_columns, axis=1)

    # -------- Throughput
    tp = aggr[aggr.tplat == 'tp_stable'].sort_values('var')

    i = 0
    for typee, group in tp.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            grp.plot(ax=ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 10000))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel('sustainable throughput [tr/s]')
        ax.set_xlabel('number of states/t-graphs')

        i += 1
        savefig('topologies_' + typee.lower() + '_tp', fig)

    # -------- Latency
    lat = aggr[aggr.tplat == 'lat_unloaded'].sort_values('var')

    i = 0
    for typee, group in lat.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            grp.plot(ax=ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 50))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel('average latency [ms]')
        ax.set_xlabel('number of states/t-graphs')

        i += 1
        savefig('topologies_' + typee.lower() + '_lat', fig)
