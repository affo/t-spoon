import common as cmn
import pandas as pd
import matplotlib.pyplot as plt

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    def fix_columns(row):
        chain = 'CHAIN' if row.tag1 == 'series' else 'PARALLEL'
        single = 'SINGLE' if row.tag2 == '1tg' else 'MULTI'
        tag = chain + '-' + single
        strategy = 'LB-' if row.strategy == 'PESS' else 'TB-'
        strategy += row.isolationLevel
        s = pd.Series({
            'value': row['value'],
            'strategy': strategy,
            'var': row['var'],
            'type': tag,
            'tplat': row.tag3
        })
        return s

    df = df[(df.isolationLevel != 'PL0') & (df.isolationLevel != 'PL1')]
    df = df[(df.tag1 != 'query') & (df.tag1 != 'ks')]
    df = df.apply(fix_columns, axis=1)

    # -------- Throughput
    tp = df[df.tplat == 'tp'].sort_values('var')

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
        cmn.savefig('topologies_' + typee.lower() + '_tp', fig)

    # -------- Latency
    lat = df[df.tplat == 'lat'].sort_values('var')

    i = 0
    for typee, group in lat.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            grp.plot(ax=ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 100))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel('average latency [ms]')
        ax.set_xlabel('number of states/t-graphs')

        i += 1
        cmn.savefig('topologies_' + typee.lower() + '_lat', fig)
