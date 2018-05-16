import sys, json, os
from common import *

def load(fname):
    with open(fname) as fp:
        result = json.load(fp)

    parsed = ExperimentResult(result)
    return parsed

# -------------------- MAIN --------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Provide file name, please'
        sys.exit(1)

    file_name = sys.argv[1]
    parsed = load(file_name)

    print 'Throughput stable', parsed.tp_stable, '[r/s]'
    print 'Latency unloaded', parsed.latency_unloaded, '[ms]'
    print 'Latency at tp-stable'
    print parsed.latency_at_tp_stable_description
