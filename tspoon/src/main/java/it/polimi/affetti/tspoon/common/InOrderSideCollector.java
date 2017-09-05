package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 17/08/17.
 */
public class InOrderSideCollector<T, X> extends SafeCollector<T> {
    private long lastRemoved = 0L;
    private final OrderedElements<Tuple2<Long, Iterable<X>>> elements;

    public InOrderSideCollector(Output<StreamRecord<Enriched<T>>> out) {
        super(out);
        this.elements = new OrderedElements<>(t -> t.f0);
    }

    // need to synchronize to send in order
    public synchronized void collectInOrder(OutputTag<X> tag, X element, Long timestamp) {
        this.collectInOrder(tag, Collections.singleton(element), timestamp);
    }

    /**
     * For batches.
     *
     * @param tag
     * @param element
     * @param timestamp
     */
    public synchronized void collectInOrder(OutputTag<X> tag, Iterable<X> element, Long timestamp) {
        elements.addInOrder(Tuple2.of(timestamp, element));
        List<Tuple2<Long, Iterable<X>>> toCollect = elements.removeContiguousWith(lastRemoved);

        for (Tuple2<Long, Iterable<X>> t : toCollect) {
            for (X e : t.f1) {
                safeCollect(tag, e);
            }
            lastRemoved = t.f0;
        }
    }
}
