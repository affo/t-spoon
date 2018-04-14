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

    # ------ throughput
    tp = aggr[(aggr.tag1 == 'query') & (aggr.tag3 == 'tp_stable')]
    tp = tp.sort_values('var')
    partitions = aggr[(aggr.tag1 == 'query') & (aggr.tag3 == 'avg_partitions')]
    partitions = partitions.sort_values('var')

    fig, ax = plt.subplots()
    ln_tp, = ax.plot(tp['var'].tolist(), tp['value'].tolist(), '-')

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('sustainable throughput [tr/s]')
    ax.set_xlabel(' ')
    ax.set_xscale('log')

    ax2 = ax.twinx()
    ln_part, = ax2.plot(partitions['var'].tolist(), partitions['value'].tolist(), '-', color='r')
    ax2.set_ylabel('average number of partitions')

    lns = (ln_tp, ln_part)
    labs = ('throughput', 'partitions')
    ax.legend(lns, labs, loc='upper center')
    #plt.gca().invert_xaxis()
    savefig('query_tp', fig)

    # ------ latency
    lat = aggr[(aggr.tag1 == 'query') & (aggr.tag3 == 'lat_stable')]
    lat = lat.sort_values('var')

    fig, ax = plt.subplots()
    lat.plot(ax=ax, kind='line', x='var', y='value')

    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)
    ax.set_ylabel('average latency [ms]')
    ax.set_xlabel(' ')
    ax.set_xscale('log')
    #plt.gca().invert_xaxis()
    savefig('query_lat', fig)
