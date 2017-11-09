package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.ListIterator;

/**
 * Created by affo on 17/08/17.
 */
public class InOrderSideCollector<T, X> extends SafeCollector<T> {
    private final OrderedElements<Tuple2<Long, Iterable<X>>> elements;
    private final OutputTag<X> tag;

    public InOrderSideCollector(Output<StreamRecord<Enriched<T>>> out, OutputTag<X> tag) {
        super(out);
        this.tag = tag;
        this.elements = new OrderedElements<>(t -> t.f0);
    }

    // need to synchronize to send in order
    public synchronized void collectInOrder(X element, Long timestamp) {
        this.collectInOrder(Collections.singleton(element), timestamp);
    }

    /**
     * For batches.
     *
     * @param element
     * @param timestamp
     */
    public synchronized void collectInOrder(Iterable<X> element, Long timestamp) {
        elements.addInOrder(Tuple2.of(timestamp, element));
    }

    /**
     * @param timestampThreshold inclusive threshold
     */
    public synchronized void flushOrdered(long timestampThreshold) {
        ListIterator<Tuple2<Long, Iterable<X>>> iterator = elements.iterator();

        while (iterator.hasNext()) {
            Tuple2<Long, Iterable<X>> next = iterator.next();

            if (next.f0 > timestampThreshold) {
                break;
            }

            iterator.remove();
            for (X e : next.f1) {
                safeCollect(tag, e);
            }
        }
    }

    public synchronized int getCount() {
        return elements.size();
    }
}
