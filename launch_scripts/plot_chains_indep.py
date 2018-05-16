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

    out_fname = os.path.join(folder_name, 'parsed_aggregates.json')
    aggr = pd.read_json(out_fname)

    def savefig(label, figure):
        figure.savefig(os.path.join(img_folder, label + '.png'))
        plt.close(figure)

    def map_fn(tag2):
        return 'SINGLE' if tag2 == '1tg' else 'MULTI'

    aggr['tag2'] = aggr['tag2'].map(map_fn)
    aggr = aggr[(aggr.strategy == 'OPT') & (aggr.isolationLevel == 'PL3')]

    # ------ throughput
    for tpe in (['tp_stable', 'sustainable throughput [tr/s]'], ['lat_stable', 'average latency [ms]']):
        data = aggr[(aggr.tag1 != 'query') & (aggr.tag1 != 'ks') & (aggr.tag3 == tpe[0])]
        data = data.sort_values('var')

        for chain, group in data.groupby('tag1'):
            fig, ax = plt.subplots()

            for single, grp in group.groupby('tag2'):
                grp.plot(ax=ax, kind='line', x='var', y='value', label=single)

            ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
            ax.margins(y=0.1)
            ax.set_ylabel(tpe[1])
            ax.set_xlabel(' ')
            savefig('top_' + chain + '_' + tpe[0].split('_')[0], fig)
