package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.Report;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Created by affo on 07/06/17.
 */
public class ThroughputCalculator<T> extends RichMapFunction<T, T> {
    public static final String ELAPSED_TIME_ACC = "elapsed-time";
    public static final String THROUGHPUT_ACC = "throughput";
    public static final String COUNTER_ACC = "number-of-elements-at-sink";
    private final int batchSize;

    private Long lastTS;
    private IntCounter count = new IntCounter();
    private MetricAccumulator elapsedTime = new MetricAccumulator();
    private MetricAccumulator throughput = new MetricAccumulator();

    public ThroughputCalculator(int batchSize) {
        this.batchSize = batchSize;
        Report.registerAccumulator(ELAPSED_TIME_ACC);
        Report.registerAccumulator(THROUGHPUT_ACC);
        Report.registerAccumulator(COUNTER_ACC);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(ELAPSED_TIME_ACC, elapsedTime);
        getRuntimeContext().addAccumulator(THROUGHPUT_ACC, throughput);
        getRuntimeContext().addAccumulator(COUNTER_ACC, count);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public T map(T element) throws Exception {
        int c = count.getLocalValuePrimitive();
        count.add(1);

        if (c == 0) {
            // this is the first one
            lastTS = System.currentTimeMillis();
        }
        c++;

        if (c % batchSize == 0) {
            long now = System.currentTimeMillis();
            double elapsedTime = (double) (now - lastTS);
            this.elapsedTime.add(elapsedTime);
            this.throughput.add(batchSize / (elapsedTime / 1000));
            lastTS = now;
        }
        return element;
    }
}
