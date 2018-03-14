import sys, os, math, shutil
import pandas as pd
import matplotlib.pyplot as plt

# frames is supposed to be a list of couples:
#   (exp_label, frame)
def plot_figure(chart_label, frames, plot_fn, columns=2):
    rows = int(math.ceil(len(frames) / float(columns)))
    fig, axarr = plt.subplots(rows, columns)
    fig.tight_layout()
    fig.canvas.set_window_title(chart_label)

    row = 0
    col = 0
    for key, frame in frames:
        ax = axarr[row, col]
        ax.set_title(str(key))
        plot_fn(ax, frame)
        ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
        ax.margins(y=0.1)
        col += 1
        col = col % columns
        if col == 0:
            row += 1

    while row < rows:
        while col < columns:
            axarr[row, col].axis('off')
            col += 1
        row += 1

    return chart_label, fig


def plot_single_figure(chart_label, label, data, plot_fn):
    fig, ax = plt.subplots()
    fig.canvas.set_window_title(chart_label)
    ax.set_title(str(label))
    plot_fn(ax, data)
    ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
    ax.margins(y=0.1)

    return chart_label, fig


# plot by strategy + isolation_level + tag1 (compare 1tg VS ntg):
#   OPT-PL0-series -> 1tg vs ntg
#   OPT-PL1-series -> ...
#   ...
#   OPT-PL1-parallel -> ...
def get_1tgVSntg_frames(aggr):
    frames = {}
    filtered = aggr[(aggr.tag1 == 'series') | (aggr.tag1 == 'parallel')] # no ks or query

    for key, group in filtered.groupby(['tag3', 'tag1', 'strategy', 'isolationLevel']):
        kpi = frames.get(key[0])
        if not kpi:
            kpi = {}
            frames[key[0]] = kpi

        sp = kpi.get(key[1])
        if not sp:
            sp = []
            kpi[key[1]] = sp

        sp.append((key[2] + '-' + key[3], group))

    return frames


# plot by experiment type (compare strategy + isolation level):
#   series-1tg -> OPT-PL0 vs OPT-PL1 ... vs PESS-PL4
#   parallel-1tg -> ...
#   series-ntg -> ...
#   ...
def get_VSstrategy(aggr):
    def merge_tags(row):
        tag = row.tag1 if row.tag2 is None else '-'.join([row.tag1, row.tag2])
        s = pd.Series({
            'value': row.value,
            'strategy': '-'.join([row.strategy, row.isolationLevel]),
            'var': row['var'],
            'tag': tag,
            'type': row.tag3
        })
        return s

    frames = {}
    merged = aggr.apply(merge_tags, axis=1)

    for key, group in merged.groupby(['type', 'tag']):
        kpi = frames.get(key[0])
        if not kpi:
            kpi = []
            frames[key[0]] = kpi

        kpi.append((key[1], group))

    return frames


# plot the curves by strategy + isolation level + exp type
# in the same graph we will have every curve for every topological configuration
#   OPT-PL?-series-1tg -> 1 state vs 2 state vs 3 state vs ...
#   OPT-PL?-parallel-1tg -> 1 state vs 2 state vs 3 state vs ...
#   OPT-PL?-series-ntg -> 1 tg vs 2 tg vs 3 tg vs ...
#   ...
#   OPT-PL?-keyspace -> 100k vs 10k vs 100 vs ...
# NOTE: For queries we should compare only isolation levels at a fixed strategy
def get_curves(data):
    def merge_tags(row):
        strategy = '-'.join([row.strategy, row.isolationLevel])
        var = row['var']

        if row.tag1 == 'query' and row.tag2 == 'fixed':
            var = strategy # display variation in strategy
            tag = 'query-fixed'
        else:
            tag = strategy
            tag += '-' + (row.tag1 if row.tag2 is None else '-'.join([row.tag1, row.tag2]))

        s = pd.Series({
            'inRate': row.inRate,
            'value': row.value,
            'var': var,
            'tag': tag,
        })
        return s

    merged = data.apply(merge_tags, axis=1)
    return [(key, group) for key, group in merged.groupby('tag')]


# ------------ MAIN ------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]
    img_folder = os.path.join(folder_name, 'img')

    if os.path.exists(img_folder):
        shutil.rmtree(img_folder)

    os.mkdir(img_folder)
    out_fname = os.path.join(folder_name, 'parsed')
    tp = pd.read_json(out_fname + '_throughput.json')
    lat = pd.read_json(out_fname + '_latency.json')
    lat_des = pd.read_json(out_fname + '_latency_description.json')
    aggr = pd.read_json(out_fname + '_aggregates.json')

    def savefig(label, figure):
        figure.savefig(os.path.join(img_folder, label + '.png'))
        plt.close(figure)

    # -------- 1tgVSntg --------
    def plot_1tgVSntg(ax, data):
        for key, group in data.groupby('tag2'):
            group = group.sort_values('var')
            ax = group.plot(ax=ax, kind='line', x='var', y='value', label=key)
            ax.set_xlabel('')

    frames = get_1tgVSntg_frames(aggr[aggr.tag1 != 'query'])
    for k, v in frames.iteritems():
        for sk, sv in v.iteritems():
            label, fig = plot_figure(k + '-' + sk, sv, plot_1tgVSntg, 4)
            savefig(label, fig)
            print '>>> Figure saved: ' + label

    # -------- VSstrategy --------
    def plot_VSstrategy(ax, data):
        for key, group in data.groupby('strategy'):
            group = group.sort_values('var')
            ax = group.plot(ax=ax, kind='line', x='var', y='value', label=key)
            ax.set_xlabel('')

    frames = get_VSstrategy(aggr[aggr.tag1 != 'query'])
    for k, v in frames.iteritems():
        label, fig = plot_figure(k, v, plot_VSstrategy, 3)
        savefig(label, fig)
        print '>>> Figure saved: ' + label

    # -------- curves --------
    def plot_curve(ax, data):
        for key, group in data.groupby('var'):
            ax = group.plot(ax=ax, kind='line', x='inRate', y='value', label=key)
            ax.set_xlabel('')

    frames = get_curves(tp[tp.inRate > 0].sort_values('inRate'))
    # 1 figure per frame
    for k, v in frames:
        label, fig = plot_single_figure('throughput-curves-' + k, k, v, plot_curve)
        savefig(label, fig)
        print '>>> Figure saved: ' + label

    frames = get_curves(lat[lat.inRate > 0].sort_values('inRate'))
    for k, v in frames:
        label, fig = plot_single_figure('latency-curves-' + k, k, v, plot_curve)
        savefig(label, fig)
        print '>>> Figure saved: ' + label
