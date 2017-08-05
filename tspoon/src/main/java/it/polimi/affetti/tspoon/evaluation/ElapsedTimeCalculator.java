package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.Report;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Created by affo on 07/06/17.
 */
public class ElapsedTimeCalculator<T> extends RichMapFunction<T, T> {
    public static final String ELAPSED_TIME_ACC = "elapsed-time";
    public static final String COUNTER_ACC = "number-of-elements-at-sink";
    private int batchSize;

    private Long lastTS;
    private IntCounter count = new IntCounter();
    private MetricAccumulator elapsedTime = new MetricAccumulator();

    public ElapsedTimeCalculator(int batchSize) {
        this.batchSize = batchSize;
        Report.registerAccumulator(ELAPSED_TIME_ACC);
        Report.registerAccumulator(COUNTER_ACC);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(ELAPSED_TIME_ACC, elapsedTime);
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
            Tuple2<Integer, Long> out = Tuple2.of(c, now - lastTS);
            elapsedTime.add(Double.valueOf(out.f1));
            lastTS = now;
        }
        return element;
    }
}
