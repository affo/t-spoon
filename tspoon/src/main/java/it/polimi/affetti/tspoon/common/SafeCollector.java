package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 20/07/17.
 */
public class SafeCollector<T, X> {
    private Output<StreamRecord<Enriched<T>>> out;
    private StreamRecord<Enriched<T>> streamRecord;
    private OutputTag<X> outputTag;

    public SafeCollector(
            Output<StreamRecord<Enriched<T>>> out,
            OutputTag<X> outputTag,
            StreamRecord<Enriched<T>> streamRecord) {
        this.out = out;
        this.streamRecord = streamRecord;
        this.outputTag = outputTag;
    }

    public synchronized void safeCollect(StreamRecord<Enriched<T>> streamRecord) {
        this.streamRecord = streamRecord;
        out.collect(streamRecord);
    }

    public synchronized void safeCollect(Enriched<T> element) {
        out.collect(streamRecord.replace(element));
    }

    public synchronized void safeCollect(X element) {
        out.collect(outputTag, streamRecord.replace(element));
    }
}
