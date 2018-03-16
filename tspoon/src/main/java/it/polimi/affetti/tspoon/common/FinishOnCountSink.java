package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.WithJobControlClient;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

/**
 * Created by affo on 26/07/17.
 */
public class FinishOnCountSink<T> extends RichSinkFunction<T> {
    public static final String COUNTER_ACC = "number-of-elements-at-sink";
    private final IntCounter counter;

    private transient Logger LOG;
    public static final int DEFAULT_LOG_EVERY = 10000;
    private final int logEvery;
    private boolean severe = true;

    private final int threshold;

    private transient WithJobControlClient jobControlClient;

    public FinishOnCountSink(int count) {
        this(count, DEFAULT_LOG_EVERY);
    }

    public FinishOnCountSink(int count, int logEvery) {
        this.counter = new IntCounter();
        this.threshold = count;
        this.logEvery = logEvery;

        Report.registerAccumulator(COUNTER_ACC);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(FinishOnCountSink.class.getSimpleName());

        jobControlClient = new WithJobControlClient();
        jobControlClient.open(getRuntimeContext());

        getRuntimeContext().addAccumulator(COUNTER_ACC, counter);
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    @Override
    public void invoke(T t) throws Exception {
        counter.add(1);
        int count = counter.getLocalValuePrimitive();

        if ((count - 1) % logEvery == 0) {
            LOG.info((threshold - count) + " records remaining");
        }

        if (severe && count > threshold) {
            throw new RuntimeException("Counted too much: " + count + ", record: " + t);
        }

        if (count == threshold) {
            jobControlClient.getJobControlClient().publishFinishMessage();
        }
    }

    public void notSevere() {
        this.severe = false;
    }
}
