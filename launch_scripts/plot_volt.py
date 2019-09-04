import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide path to volt results (.csv), please'
        sys.exit(1)

    fname = sys.argv[1]
    df = pd.read_csv(fname, names=['ts', 'percentage', 'tp', 'lat'])
    df = df.sort_values('percentage')
    df = df.reset_index(drop=True)

    # aggregate
    grouped = df.groupby('percentage')
    tp = pd.DataFrame(pd.concat([
        pd.Series(name),
        group.tp.describe().rename(name),
        ]) for name, group in grouped)
    tp.rename(columns={tp.columns[0]: 'var', 'mean': 'value'}, inplace=True)
    lat = pd.DataFrame(pd.concat([
        pd.Series(name),
        group.lat.describe().rename(name),
        ]) for name, group in grouped)
    lat.rename(columns={lat.columns[0]: 'var', 'mean': 'value'}, inplace=True)

    #print tp
    #print lat
    #sys.exit(1)

    # ------ throughput
    cmn.reset_colors_and_markers()
    fig, ax = plt.subplots()
    #for key, group in df.groupby('percentage'):
    #    cmn.my_plot(group, ax, kind='line', x='percentage', y='tp', label=key)
    cmn.my_plot(tp, ax, kind='line', x='var', y='value')

    #ax.set_ylim((0, 80000))
    ax.set_xlim((-10, 110))
    ax.margins(y=0.1)
    ax.set_ylabel("Average throughput [Ktxn/s]")
    ax.set_xlabel('Single partition transactions percentage')
    ax.set_yscale('log')
    #plt.gca().invert_yaxis()
    ax.get_legend().remove()
    cmn.savefig('volt_tp', fig)

    # ------ latency
    cmn.reset_colors_and_markers()

    fig, ax = plt.subplots()
    #for key, group in df.groupby('percentage'):
    #    cmn.my_plot(group, ax, kind='line', x='percentage', y='lat', label=key)
    cmn.my_plot(lat, ax, kind='line', x='var', y='value')

    #ax.set_ylim((0, 8))
    ax.set_xlim((-10, 110))
    ax.margins(y=0.1)
    ax.set_ylabel("Average latency [ms]")
    ax.set_xlabel('Single partition transactions percentage')
    ax.set_yscale('log')
    #plt.gca().invert_yaxis()
    ax.get_legend().remove()
    cmn.savefig('volt_lat', fig)
