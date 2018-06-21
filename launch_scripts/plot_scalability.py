import common as cmn
import pandas as pd
import matplotlib.pyplot as plt

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    df = df[df['tag1'] == 'scale']
    df['tplat'] = df['tag3']

    # -------- Throughput
    cmn.reset_colors_and_markers()
    tp = df[df.tplat == 'tp'].sort_values('var')

    fig, ax = plt.subplots()
    cmn.my_plot(tp, ax, kind='line', x='var', y='value', legend=False)

    ax.set_ylim((0, 15000))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.TP_LABEL)
    ax.set_xlabel('Graph parallelism')

    cmn.savefig('scalability_tp', fig)

    # -------- Latency
    cmn.reset_colors_and_markers()
    lat = df[df.tplat == 'lat'].sort_values('var')

    fig, ax = plt.subplots()
    cmn.my_plot(lat, ax, kind='line', x='var', y='value', legend=False)

    ax.set_ylim((0, 50))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.LAT_LABEL)
    ax.set_xlabel('Graph parallelism')

    cmn.savefig('scalability_lat', fig)
