import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    def map_fn(tag2):
        return 'SINGLE' if tag2 == '1tg' else 'MULTI'

    df['tag2'] = df['tag2'].map(map_fn)
    df = df[(df.strategy == 'OPT') & (df.isolationLevel == 'PL3')]

    def myplot(tag3, ylabel):
        data = df[(df.tag1 != 'query') & (df.tag1 != 'ks') & (df.tag3 == tag3)]
        data = data.sort_values('var')

        for chain, group in data.groupby('tag1'):
            fig, ax = plt.subplots()

            for single, grp in group.groupby('tag2'):
                grp.plot(ax=ax, kind='line', x='var', y='value', label=single)

            ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
            ax.margins(y=0.1)
            ax.set_ylabel(ylabel)
            ax.set_xlabel(' ')
            cmn.savefig('top_' + chain + '_' + tag3, fig)

    myplot('tp', cmn.TP_LABEL)
    myplot('lat', cmn.LAT_LABEL)
