import sys, json, os
import matplotlib.pyplot as plt

# prefixes
SERIES_1TG = 'series_1tg_'
SERIES_NTG = 'series_ntg_'
PARALLEL_1TG = 'parallel_1tg_'
PARALLEL_NTG = 'parallel_ntg_'
KEYSPACE = 'keyspace_'


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide folder name, please'
        sys.exit(1)

    folder_name = sys.argv[1]

    def join(fname):
        return os.path.join(folder_name, fname)

    def load_json(fname):
        path = join(fname + '.json')
        with open(path) as fp:
            return json.load(fp)

    # for series and parallel, I want to compare 1tg against ntg.
    # on the x-axis I have the number of states/tgs, while on the y-axis
    # I have throughput and latency

    # returns a couple of (throughputs, latencies)
    def get_ys(x_axis, prefix):
        throughputs = []
        latencies = []
        max_or_avg = False

        for x in x_axis:
            tp = None
            lat = None
            try:
                data = load_json(prefix + str(x))
                tp_and_lat = data['accumulators'].get('max-throughput-and-latency')

                if tp_and_lat is not None:
                    max_or_avg = True
                    tp = tp_and_lat['max-throughput']
                    lat = tp_and_lat['latency-at-max-throughput']
                else:
                    tp = data['accumulators']['throughput']['mean']
                    lat = data['accumulators'].get('latency')

                    # backward compatibility
                    if lat is None:
                        lat = data['accumulators']['timestamp-deltas']['latency']['mean']
                    else:
                        lat = lat['mean']

            except Exception as e:
                print "Some exception happened:", e
            finally:
                throughputs.append(tp)
                latencies.append(lat)

        tp_label = '{} [records/s]'
        lat_label = '{} [ms]'

        if max_or_avg:
            tp_label = tp_label.format('Maximum throughput')
            lat_label = lat_label.format('Latency at maximum throughput')
        else:
            tp_label = tp_label.format('Maximum input throughput')
            lat_label = lat_label.format('Mean latency')

        return throughputs, latencies, tp_label, lat_label

    xlabel = 'Number of states/tgraphs'

    def plot_stuff(x, y_onetg, y_ntgs, ylabel, filename):
        fig, ax = plt.subplots()
        ax.plot(x, y_onetg, 'b-', marker='x', label='1tg')
        ax.plot(x, y_ntgs, 'r-', marker='x', label='ntg')
        ax.xaxis.set_ticks(x) # integer ticks
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.legend(loc='upper right')
        fig.savefig(filename)

    x_axis = [1, 2, 3, 4, 5]
    for label in [('series', SERIES_1TG, SERIES_NTG), ('parallel', PARALLEL_1TG, PARALLEL_NTG)]:
        tps1, lats1, tp_label, lat_label = get_ys(x_axis, label[1])
        tpsn, latsn, _, _ = get_ys(x_axis, label[2])

        plot_stuff(x_axis, tps1, tpsn, tp_label, join(label[0] + '_throughput.png'))
        plot_stuff(x_axis, lats1, latsn, lat_label, join(label[0] + '_latency.png'))


    factors = [10 ** i for i in xrange(1, 5)]
    keyspace_x_axis = [base * factor for factor in factors for base in [1, 4, 7]]
    keyspace_x_axis.append(10 ** 5)
    ks_throughput, ks_latency, _, _ = get_ys(keyspace_x_axis, KEYSPACE)

    fig, ax = plt.subplots()
    ax.plot(keyspace_x_axis, ks_throughput, 'b-', marker='x')
    ax.set_xlabel('Keyspace dimension')
    ax.set_ylabel('Maximum input throughput (records/s)')
    fig.savefig(join('keyspace_throughput.png'))

    fig, ax = plt.subplots()
    ax.plot(keyspace_x_axis, ks_latency, 'b-', marker='x')
    ax.set_xlabel('Keyspace dimension')
    ax.set_ylabel('Average latency (ms)')
    fig.savefig(join('keyspace_latency.png'))

