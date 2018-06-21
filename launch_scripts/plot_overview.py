import common as cmn
import pandas as pd
import matplotlib.pyplot as plt

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    df = df[(df['tag1'] == 'series') | (df['tag1'] == 'parallel')]
    df['tag1'] = df['tag1'].apply(lambda v: 'CHAIN' if v == 'series' else 'PARALLEL')
    df['tag2'] = df['tag2'].apply(lambda v: 'SINGLE' if v == '1tg' else 'MULTI')
    df['type'] = df['tag1'] + '-' + df['tag2']
    df['strategy'] = df['strategy'].apply(lambda v: 'LB' if v == 'PESS' else 'TB')
    df['strategy'] = df['strategy'] + '-' + df['isolationLevel']
    df['tplat'] = df['tag3']

    def get_xlabel(typee):
        return 'Number of stateful operators' if 'SINGLE' in typee else 'Number of t-graphs'

    # -------- Throughput
    cmn.reset_colors_and_markers()
    tp = df[df.tplat == 'tp'].sort_values('var')

    i = 0
    for typee, group in tp.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            cmn.my_plot(grp, ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 15000))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel(cmn.TP_LABEL)
        ax.set_xlabel(get_xlabel(typee))

        i += 1
        cmn.savefig_with_separate_legend('topologies_' + typee.lower() + '_tp', ax, fig)

    # -------- Latency
    cmn.reset_colors_and_markers()
    lat = df[df.tplat == 'lat'].sort_values('var')

    i = 0
    for typee, group in lat.groupby('type'):
        fig, ax = plt.subplots()

        for strategy, grp in group.groupby('strategy'):
            cmn.my_plot(grp, ax, kind='line', x='var', y='value', label=strategy)

        ax.set_ylim((0, 50))
        ax.margins(y=0.1)
        ax.set_xticks(range(1, 6))
        ax.set_ylabel(cmn.LAT_LABEL)
        ax.set_xlabel(get_xlabel(typee))

        i += 1
        cmn.savefig_with_separate_legend('topologies_' + typee.lower() + '_lat', ax, fig)
