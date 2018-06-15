import common as cmn
import pandas as pd
import matplotlib.pyplot as plt

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    df = df[(df.tag1 != 'query') & (df.tag1 != 'ks')]
    df['tag1'] = df['tag1'].apply(lambda v: 'CHAIN' if v == 'series' else 'PARALLEL')
    df['tag2'] = df['tag2'].apply(lambda v: 'SINGLE' if v == '1tg' else 'MULTI')
    df['type'] = df['tag1'] + '-' + df['tag2']
    df['strategy'] = df['strategy'].apply(lambda v: 'LB' if v == 'PESS' else 'TB')
    df['strategy'] = df['strategy'] + '-' + df['isolationLevel']
    df['tplat'] = df['tag3']

    # -------- Throughput
    tp = df[df.tplat == 'tp'].sort_values('var')

    i = 0
    for typee, group in tp.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            cmn.my_plot(grp, ax=ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 15000))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel('sustainable throughput [tr/s]')
        ax.set_xlabel('number of states/t-graphs')

        i += 1
        cmn.savefig('topologies_' + typee.lower() + '_tp', fig)

    # -------- Latency
    lat = df[df.tplat == 'lat'].sort_values('var')

    i = 0
    for typee, group in lat.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            cmn.my_plot(grp, ax=ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 100))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel('average latency [ms]')
        ax.set_xlabel('number of states/t-graphs')

        i += 1
        cmn.savefig('topologies_' + typee.lower() + '_lat', fig)
