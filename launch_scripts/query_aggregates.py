import sys, os, math, shutil
import pandas as pd
import common as cmn

# ------------ MAIN ------------
if __name__ == '__main__':
    df = cmn.load_parsed_results()

    format = '[TB|LB]-[PL*]-[(series|parallel)(_1tg|_ntg)|(ks|query|scale)]-var'
    if len(sys.argv) < 3:
        print 'Provide query in the format', format
        sys.exit(1)

    query = sys.argv[2]
    print '>>> Format:', format
    print '>>> Query:', query

    def key_fn(row):
        strategy = 'LB' if row['strategy'] == 'PESS' else 'TB'
        isol = row['isolationLevel']
        tag = row['tag1']
        tag2 = row['tag2']
        var = row['var']
        if tag2 != 'None':
            tag += '_' + tag2

        return '-'.join([strategy, isol, tag, str(var)])

    df['key'] = df.apply(key_fn, axis=1)
    print df[df['key'] == query][['key', 'tag3', 'value', 'std', 'count']]
