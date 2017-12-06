package it.polimi.affetti.tspoon.common;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 01/08/17.
 */
public abstract class TimestampTracker<T> extends ProcessFunction<T, T> {
    // <metricName, isBegin, recordId>
    private final OutputTag<Tuple3<String, Boolean, String>> recordTracking;
    private final String metricName;
    private final boolean isBegin;

    public TimestampTracker(String metricName, boolean isBegin) {
        this.metricName = metricName;
        this.isBegin = isBegin;
        String outputId = metricName + "." + (isBegin ? "start" : "end");
        this.recordTracking = new OutputTag<Tuple3<String, Boolean, String>>(outputId) {
        };
    }

    public OutputTag<Tuple3<String, Boolean, String>> getRecordTracking() {
        return recordTracking;
    }

    protected abstract String extractId(T element);

    @Override
    public void processElement(T t, Context context, Collector<T> collector) throws Exception {
        String id = extractId(t);
        context.output(recordTracking, Tuple3.of(metricName, isBegin, id));
        collector.collect(t);
    }
}
