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

        for x in x_axis:
            tp = None
            lat = None
            try:
                data = load_json(prefix + str(x))
                tp = data['accumulators']['throughput']['mean']
                lat = data['accumulators']['timestamp-deltas']['latency']['mean']
            except Exception as e:
                print "Some exception happened:", e
            finally:
                throughputs.append(tp)
                latencies.append(lat)

        return throughputs, latencies


    def plot_throughput(x, tp1, tpn, label):
        fig, ax = plt.subplots()
        ax.plot(x, tp1, 'b-', marker='x', label='1tg')
        ax.plot(x, tpn, 'r-', marker='x', label='ntg')
        ax.xaxis.set_ticks(x) # integer ticks
        ax.set_xlabel('Number of states/tgraphs')
        ax.set_ylabel('Maximum input throughput (records/s)')
        ax.legend(loc='upper right')
        fig.savefig(join(label + '_throughput.png'))

    def plot_latency(x, lat1, latn, label):
        fig, ax = plt.subplots()
        ax.plot(x, lat1, 'b-', marker='x', label='1tg')
        ax.plot(x, latn, 'r-', marker='x', label='ntg')
        ax.xaxis.set_ticks(x) # integer ticks
        ax.set_xlabel('Number of states/tgraphs')
        ax.set_ylabel('Average latency (ms)')
        ax.legend(loc='upper left')
        fig.savefig(join(label + '_latency.png'))

    x_axis = list(xrange(1, 6))
    one_tg_tps, one_tg_lats = get_ys(x_axis, SERIES_1TG)
    ntg_tps, ntg_lats = get_ys(x_axis, SERIES_NTG)

    plot_throughput(x_axis, one_tg_tps, ntg_tps, 'series')
    plot_latency(x_axis, one_tg_lats, ntg_lats, 'series')

    one_tg_tps, one_tg_lats = get_ys(x_axis, PARALLEL_1TG)
    ntg_tps, ntg_lats = get_ys(x_axis, PARALLEL_NTG)

    plot_throughput(x_axis, one_tg_tps, ntg_tps, 'parallel')
    plot_latency(x_axis, one_tg_lats, ntg_lats, 'parallel')

    factors = [10 ** i for i in xrange(1, 5)]
    keyspace_x_axis = [base * factor for factor in factors for base in [1, 4, 7]]
    keyspace_x_axis.append(10 ** 5)
    ks_throughput, ks_latency = get_ys(keyspace_x_axis, KEYSPACE)

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

