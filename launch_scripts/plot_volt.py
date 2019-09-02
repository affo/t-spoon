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

    # ------ throughput
    cmn.reset_colors_and_markers()
    fig, ax = plt.subplots()
    for key, group in df.groupby('percentage'):
        cmn.my_plot(group, ax, kind='line', x='percentage', y='tp', label=key)

    ax.set_ylim((0, 15000))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.TP_LABEL)
    ax.set_xlabel('Single partition transactions percentage')
    ax.set_xscale('log')
    plt.gca().invert_xaxis()
    cmn.savefig('volt_tp', fig)

    # ------ latency
    cmn.reset_colors_and_markers()

    fig, ax = plt.subplots()
    for key, group in df.groupby('percentage'):
        cmn.my_plot(group, ax, kind='line', x='percentage', y='lat', label=key)

    ax.set_ylim((0, 50))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.LAT_LABEL)
    ax.set_xlabel('Single partition transactions percentage')
    ax.set_xscale('log')
    plt.gca().invert_xaxis()
    cmn.savefig('volt_lat', fig)
