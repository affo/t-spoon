import sys, os, math, shutil
import pandas as pd

# Please create a folder containing only parse aggragates from multiple runs,
# the script will output the description (mean, max, min , percentiles) in the
# file "aggagg.json"

# ------------ MAIN ------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]
    aggagg = pd.DataFrame()

    # aggregates_columns = [
    #    'value', 'strategy', 'isolationLevel', 'var',
    #    'tag1', 'tag2', 'tag3'
    # ]
    # NOTE group by everything!

    filename = 'aggagg.json'
    parsed = 0
    for subdir, dirs, files in os.walk(folder_name):
        for file in files:
            if file != filename and file.endswith('.json'):
                parsed += 1
                fname = os.path.join(subdir, file)

                print '>>> Parsing', fname
                agg = pd.read_json(fname)
                aggagg = aggagg.append(agg, ignore_index=True)

    aggagg = aggagg.fillna('None')
    agg_columns = [
       'strategy', 'isolationLevel', 'var', 'tag1', 'tag2', 'tag3'
    ]
    grouped = aggagg.groupby(agg_columns)

    described = pd.DataFrame(pd.concat([pd.Series(name), group['value'].describe().rename(name)])
                         for name, group in grouped)
    indices = range(0, len(agg_columns))
    described = described.rename(dict(zip(indices, agg_columns)), axis='columns')

    out_fname = os.path.join(folder_name, filename)
    described.to_json(out_fname)

    print '>>> Output written to', out_fname
    print '>>> Parsed:', parsed
