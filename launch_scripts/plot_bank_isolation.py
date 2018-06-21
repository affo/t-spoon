import sys, os, math
import pandas as pd
import matplotlib.pyplot as plt
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    df = df[(df['tag1'] == 'series') & (df['tag2'] == 'ntg') & (df['var'] == 1)]
    df['strategy'] = df['strategy'].apply(lambda v: 'LB' if v == 'PESS' else 'TB')

    lb = df[(df.strategy == 'LB')]
    tb = df[(df.strategy == 'TB')]

    def apply_fn(row):
        tplat = row.tag3
        isolation = row.isolationLevel
        value = row.value

        return pd.Series([tplat, isolation, value])

    lb = lb.apply(apply_fn, axis=1)
    lb.columns = ['tplat', 'isolation', 'LB']
    lb['key'] = lb['tplat'] + lb['isolation']
    tb = tb.apply(apply_fn, axis=1)
    tb.columns = ['tplat', 'isolation', 'TB']
    tb['key'] = tb['tplat'] + tb['isolation']
    ys = ['TB', 'LB']

    def merge_fn(row):
        tplat = row.tplat_lb if not pd.isna(row.tplat_lb) else row.tplat_tb
        isolation = row.isolation_lb if not pd.isna(row.isolation_lb) else row.isolation_tb
        tb = row.TB
        lb = row.LB
        return pd.Series([tplat, isolation, tb, lb])

    joined = lb.set_index('key').join(tb.set_index('key'), how='outer', lsuffix='_lb', rsuffix='_tb')
    joined = joined.apply(merge_fn, axis=1)
    joined.columns = ['tplat', 'isolation', 'TB', 'LB']

    colors = ['#424242', 'darkorange']
    # ------ throughput
    tp = joined[(joined.tplat == 'tp')]

    fig, ax = plt.subplots()
    tp.plot(ax=ax, kind='bar', x='isolation', y=ys, rot=0,
        color=colors, linewidth=1, width=0.35)

    cmn.set_yaxis_formatter(ax)
    ax.set_ylim((0, 15000))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.TP_LABEL)
    ax.set_xlabel(' ')
    cmn.savefig('bank_isol_tp', fig)

    # ------ latency
    lat = joined[(joined.tplat == 'lat')]

    fig, ax = plt.subplots()
    lat.plot(ax=ax, kind='bar', x='isolation', y=ys, rot=0,
        color=colors, linewidth=1, width=0.35)

    ax.set_ylim((0, 20))
    ax.margins(y=0.1)
    ax.set_ylabel(cmn.LAT_LABEL)
    ax.set_xlabel(' ')
    cmn.savefig('bank_isol_lat', fig)
