import sys, os, json
import common as cmn
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# ------------ MAIN ------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]

    x = []
    at_state = []
    at_source = []

    for subdir, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.json'):
                fname = os.path.join(subdir, file)

                print '>>> Parsing', fname
                data = None
                with open(fname) as fp:
                    data = json.load(fp)

                accs = data['accumulators']
                x.append(int(accs['no-wal-entries-replayed-at-source']['mean']))
                at_state.append(float(accs['recovery-time-at-state']['mean']))
                at_source.append(float(accs['recovery-time-at-source']['mean']))

    data = dict(replayed=x, at_state=at_state, at_source=at_source)
    df = pd.DataFrame(data=data).sort_values('replayed')

    cmn.reset_colors_and_markers()
    fig, ax = plt.subplots()

    formatter = FuncFormatter(lambda x, pos: int(x / 1000))
    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_formatter(formatter)

    cmn.my_plot(df, ax, kind='line', x='replayed', y='at_state',
        label='stateful operator', yformatter=False)
    cmn.my_plot(df, ax, kind='line', x='replayed', y='at_source',
        label='open operator', yformatter=False)

    ax.set_ylim((0, 33000))
    ax.margins(y=0.1)
    ax.set_ylabel('Replay time [s]')
    ax.set_xlabel('Number of entries in the WAL (thousands)')

    cmn.savefig('replay_simulation', fig)
