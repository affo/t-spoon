package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.Report;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Created by affo on 07/06/17.
 */
public class ThroughputCalculator<T> extends RichMapFunction<T, T> {
    public static final String ELAPSED_TIME_ACC = "elapsed-time";
    public static final String THROUGHPUT_ACC = "throughput";
    private final int batchSize;

    private Long lastTS;
    private int count = 0;
    private MetricAccumulator elapsedTime = new MetricAccumulator();
    private MetricAccumulator throughput = new MetricAccumulator();

    public ThroughputCalculator(int batchSize) {
        this.batchSize = batchSize;
        Report.registerAccumulator(ELAPSED_TIME_ACC);
        Report.registerAccumulator(THROUGHPUT_ACC);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(ELAPSED_TIME_ACC, elapsedTime);
        getRuntimeContext().addAccumulator(THROUGHPUT_ACC, throughput);
    }

    @Override
    public T map(T element) throws Exception {
        if (count == 0) {
            // this is the first one
            lastTS = System.currentTimeMillis();
        }
        count++;

        if (count % batchSize == 0) {
            long now = System.currentTimeMillis();
            double elapsedTime = (double) (now - lastTS);
            this.elapsedTime.add(elapsedTime);
            this.throughput.add(batchSize / (elapsedTime / 1000));
            lastTS = now;
        }

        return element;
    }
}
