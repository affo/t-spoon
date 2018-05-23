import sys, os, math, shutil, json
import pandas as pd
import matplotlib.pyplot as plt

def extract_row(json_content):
    try:
        config = json_content['config']['execution-config']['user-config']
        wsize = int(config['windowSizeSeconds'])
        wslide = int(config['windowSlideMilliseconds'])
        is_tg = not config['analyticsOnly'] == 'true'
        accs = json_content['accumulators']
        rate = float(accs['input-throughput']['mean'])
        before_tg = float(accs['before-tgraph-throughput']['mean'])
        comp = float(accs['window-processing-time']['mean'])
    except TypeError:
        # some None is around...
        return None

    return {
        'wsize': wsize,
        'wslide': wslide,
        'tg': is_tg,
        'inputRate': rate,
        'beforeTG': before_tg,
        'wcomp': comp
    }

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

    frame = {
        'wsize': [],
        'wslide': [],
        'tg': [],
        'inputRate': [],
        'beforeTG': [],
        'wcomp': []
    }

    for subdir, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.json'):
                fname = os.path.join(subdir, file)
                print '>>> Parsing', fname

                result = None
                with open(fname) as fp:
                    result = json.load(fp)

                row_dict = extract_row(result)
                if row_dict is None:
                    print ">>> Invalid result, skipping...", fname
                else:
                    for k, v in row_dict.iteritems():
                        frame[k].append(v)

    df = pd.DataFrame(data=frame)

    max_slide = df.wslide.max()
    min_size = df.wsize.min()
    fixed_slide = df[df.wslide == max_slide].sort_values('wsize')
    fixed_size = df[df.wsize == min_size].sort_values('wslide')

    def savefig(label, figure):
        figure.savefig(os.path.join(img_folder, label + '.png'))
        plt.close(figure)

    def plot(data, x, xlabel, y, ylabel):
        # two curves, 1 for with tg, the other without
        fig, ax = plt.subplots()
        for tg_or_not, group in data.groupby('tg'):
            label = 'with-tgraph' if tg_or_not else 'without-tgraph'
            group.plot(ax=ax, kind='line', x=x, y=y, label=label)

            ax.set_ylim((0, ax.yaxis.get_data_interval()[1]))
            ax.margins(y=0.1)
            ax.set_ylabel(ylabel)
            ax.set_xlabel(xlabel)
        savefig('mixed_' + x + '_' + y, fig)

    lab_bp_ir = 'Backpressured input rate [r/s]'
    lab_cmp_ms = 'Window computation time [ms]'
    lab_btg = 'TGraph input rate [r/s]'
    lab_wsize = 'Window size [s]'
    lab_wslide = 'Window slide [ms]'
    plot(fixed_slide, 'wsize', lab_wsize, 'inputRate', lab_bp_ir)
    plot(fixed_slide, 'wsize', lab_wsize, 'wcomp', lab_cmp_ms)
    plot(fixed_slide, 'wsize', lab_wsize, 'beforeTG', lab_btg)
    plot(fixed_size, 'wslide', lab_wslide, 'inputRate', lab_bp_ir)
    plot(fixed_size, 'wslide', lab_wslide, 'wcomp', lab_cmp_ms)
    plot(fixed_size, 'wslide', lab_wslide, 'beforeTG', lab_btg)
