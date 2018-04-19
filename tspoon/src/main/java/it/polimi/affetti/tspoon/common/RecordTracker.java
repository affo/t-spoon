package it.polimi.affetti.tspoon.common;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 01/08/17.
 */
public abstract class RecordTracker<T, ID> extends ProcessFunction<T, T> {
    // <recordId, startOrEnd>
    private final OutputTag<ID> recordTracking;

    public RecordTracker(String metricName, boolean isBegin) {
        String outputId = metricName + "." + (isBegin ? "start" : "end");
        this.recordTracking = createRecordTrackingOutputTag(outputId);
    }

    public abstract OutputTag<ID> createRecordTrackingOutputTag(String label);

    public OutputTag<ID> getRecordTracking() {
        return recordTracking;
    }

    protected abstract ID extractId(T element);

    @Override
    public void processElement(T t, Context context, Collector<T> collector) throws Exception {
        ID id = extractId(t);
        context.output(recordTracking, id);
        collector.collect(t);
    }
}
