import sys, json, os
import pandas as pd
from common import *

def load_results(folder_name):
    columns = [
        'value', 'strategy', 'isolationLevel', 'var',
        'tag1', 'tag2', 'tag3'
    ]
    data = []
    problems = []
    number_of_experiments = 0

    def extract_columns(result):
        param = result.x
        tag1 = result.experiment_type
        tag2 = None
        if tag1 not in ('query', 'ks', 'scale'):
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

        print [result.throughput] + tags + ['tp']

        data.append([result.latency] + tags + ['lat'])
        data.append([result.throughput] + tags + ['tp'])
        if result.experiment_type == 'query':
            data.append([result.avg_partitions] + tags + ['avg_partitions'])

    for subdir, dirs, files in os.walk(folder_name):
        for file in files:
            if not file.startswith('parsed') \
                and file.endswith('.json'):
                number_of_experiments += 1
                fname = os.path.join(subdir, file)
                print '>>> Parsing', fname
                parsed = load_experiment(fname)
                print parsed
                print

                if len(parsed.problems) > 0:
                    problems.append(dict(label=fname, reasons=parsed.problems))

                if parsed.valid:
                    append(parsed)


    print '>>> Number of experiments:', number_of_experiments

    df = pd.DataFrame(data, columns=columns)
    return df, problems


# -------------------- MAIN --------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]

    df, problems = load_results(folder_name)

    out_fname = os.path.join(folder_name, 'parsed_results.json')
    problems_fname = os.path.join(folder_name, 'parsed_problems.json')

    df.to_json(out_fname)
    with open(problems_fname, 'w') as fp:
        json.dump(problems, fp, indent=4, sort_keys=True)

    print '>>> Output written to parsed_results.json'
    print '>>> Problems detected:', len(problems)
