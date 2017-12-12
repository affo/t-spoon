package it.polimi.affetti.tspoon.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 01/08/17.
 */
public abstract class RecordTracker<T> extends ProcessFunction<T, T> {
    // <recordId, startOrEnd>
    private final OutputTag<Tuple2<Long, Boolean>> recordTracking;
    private final boolean isBegin;

    public RecordTracker(String metricName, boolean isBegin) {
        this.isBegin = isBegin;
        String outputId = metricName + "." + (isBegin ? "start" : "end");
        this.recordTracking = new OutputTag<Tuple2<Long, Boolean>>(outputId) {
        };
    }

    public OutputTag<Tuple2<Long, Boolean>> getRecordTrackingOutputTag() {
        return recordTracking;
    }

    protected abstract long extractId(T element);

    @Override
    public void processElement(T t, Context context, Collector<T> collector) throws Exception {
        long id = extractId(t);
        context.output(recordTracking, Tuple2.of(id, isBegin));
        collector.collect(t);
    }
}
