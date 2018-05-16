import sys, json, os
import pandas as pd
from common import *

def load_results(folder_name):
    curves_columns = [
        'inRate', 'value', 'strategy', 'isolationLevel', 'var',
        'tag1', 'tag2',
    ]
    aggregates_columns = [
        'value', 'strategy', 'isolationLevel', 'var',
        'tag1', 'tag2', 'tag3'
    ]
    lat_des_columns = [
        'count', 'mean', 'std', 'min', '25%', '50%', '75%', '90%', 'max',
        'strategy', 'isolationLevel', 'var', 'tag1', 'tag2',
    ]
    throughput_df = []
    latency_df = []
    aggregates_df = []
    latency_description_df = []
    problems = []
    number_of_experiments = 0

    def extract_columns(result):
        param = result.x
        tag1 = result.experiment_type
        tag2 = None
        if tag1 not in ('query', 'ks'):
            tag2 = str(tag1)
            tag1 = result.series_or_parallel

        if tag1 == 'query' and param is None:
            tag2 = 'fixed'

        return [
            result.strategy,
            result.isolation_level,
            param, tag1, tag2
        ]

    def append(result):
        tags = extract_columns(result)

        aggregates_df.append([result.latency_at_tp_stable] + tags + ['lat_stable'])
        aggregates_df.append([result.latency_min] + tags + ['lat_min'])
        aggregates_df.append([result.latency_unloaded] + tags + ['lat_unloaded'])
        aggregates_df.append([result.tp_max] + tags + ['tp_max'])
        aggregates_df.append([result.tp_stable] + tags + ['tp_stable'])
        if result.experiment_type == 'query':
            aggregates_df.append([result.avg_partitions] + tags + ['avg_partitions'])
        latency_description_df.append(list(result.latency_at_tp_stable_description) + tags)

        mappings = [
            (result.throughput_curve, throughput_df),
            (result.latency_curve, latency_df),
        ]
        for mapping in mappings:
            curve = mapping[0]
            frame = mapping[1]
            for point in curve:
                row = [
                    point['actualRate'],
                    point['value']
                ]
                row.extend(tags)
                frame.append(row)


    for subdir, dirs, files in os.walk(folder_name):
        for file in files:
            if not file.startswith('parsed') \
                and file.endswith('.json'):
                number_of_experiments += 1
                fname = os.path.join(subdir, file)
                print '>>> Parsing', fname

                result = None
                with open(fname) as fp:
                    result = json.load(fp)

                parsed = ExperimentResult(result)

                if parsed.problems != None:
                    problems.append(dict(label=fname, reasons=parsed.problems))

                if parsed.valid:
                    append(parsed)


    print '>>> Number of experiments:', number_of_experiments

    tp = pd.DataFrame(throughput_df, columns=curves_columns)
    lat = pd.DataFrame(latency_df, columns=curves_columns)
    aggr = pd.DataFrame(aggregates_df, columns=aggregates_columns)
    lat_des = pd.DataFrame(latency_description_df, columns=lat_des_columns)
    return tp, lat, aggr, lat_des, problems


# -------------------- MAIN --------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]

    tp, lat, aggr, lat_des, problems = load_results(folder_name)

    out_fname = os.path.join(folder_name, 'parsed')
    suffix = lambda s: '{}_{}.json'.format(out_fname, s)

    tp.to_json(suffix('throughput'))
    lat.to_json(suffix('latency'))
    lat_des.to_json(suffix('latency_description'))
    aggr.to_json(suffix('aggregates'))
    with open(suffix('problems'), 'w') as fp:
        json.dump(problems, fp, indent=4, sort_keys=True)

    print '>>> Output written to parsed_*.json'
    print '>>> Problems detected:', len(problems)
