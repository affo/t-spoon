package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 23/05/18.
 */
public class ThroughputMeter<T> extends RichSinkFunction<T> {
    private int count = 0;
    private long start = -1;
    private final String accumulatorName;

    private TransientPeriod transientPeriod;
    private MetricAccumulator throughput = new MetricAccumulator();

    public ThroughputMeter(String accumulatorName, TransientPeriod transientPeriod) {
        this.accumulatorName = accumulatorName;
        this.transientPeriod = transientPeriod;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(accumulatorName, throughput);

        transientPeriod.start();
    }

    @Override
    public void invoke(T element) throws Exception {
        if (!transientPeriod.hasFinished()) {
            return;
        }

        count++;

        if (start < 0) {
            start = System.nanoTime();
        }

        long end = System.nanoTime();
        long delta = end - start;
        if (delta >= 10 * Math.pow(10, 9)) { // every 10 seconds
            double throughput = count / (delta * Math.pow(10, -9));
            this.throughput.add(throughput);
            count = 0;
            start = end;
        }
    }
}
