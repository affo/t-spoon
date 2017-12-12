import sys, json, os
import pandas as pd
import matplotlib.pyplot as plt

# prefixes
SERIES_1TG = 'series_1tg_'
SERIES_NTG = 'series_ntg_'
PARALLEL_1TG = 'parallel_1tg_'
PARALLEL_NTG = 'parallel_ntg_'
KEYSPACE = 'keyspace_'


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide prefix, please'
        sys.exit(1)

    prefix = sys.argv[1]

    def load_json(fname):
        path = fname + '.json'
        with open(path) as fp:
            return json.load(fp)

    # for series and parallel, I want to compare 1tg against ntg.
    # on the x-axis I have the number of states/tgs, while on the y-axis
    # I have throughput and latency

    # returns a couple of (throughputs, latencies)
    def get_curves(prefix, rangee, key):
        curves = []

        for i in rangee:
            curve = None
            try:
                data = load_json(prefix + str(i))
                curve = data['accumulators'][key + '-curve']
                for point in curve:
                    throughput = point["value"]
                    del point["value"]
                    point[key] = throughput["mean"]

                curve = pd.DataFrame(curve)
            except Exception as e:
                print "Some exception happened:", e
            finally:
                curves.append(curve)

        return curves

    for key in ['throughput', 'latency']:
        curves = get_curves(prefix, [1], key)

        i = 1
        for curve in curves:
            fig, ax = plt.subplots()
            plt.gca().set_aspect('equal')
            curve.plot(ax=ax, x='expectedRate', y=key)
            curve.plot(ax=ax, x='expectedRate', y='actualRate')

            image_path = prefix + str(i) + '-' + key + '.png'
            fig.savefig(image_path)
            print 'Figure saved to', image_path
            i += 1

