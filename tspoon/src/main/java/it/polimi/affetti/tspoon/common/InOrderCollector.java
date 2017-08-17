package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.function.Function;

/**
 * Created by affo on 17/08/17.
 */
public class InOrderCollector<T, X> extends SafeCollector<T> {
    private long lastRemoved = 0L;
    private final OrderedElements<X> elements;
    private final Function<X, Long> timestampExtractor;

    public InOrderCollector(
            Output<StreamRecord<Enriched<T>>> out, Function<X, Long> timestampExtractor) {
        super(out);
        this.timestampExtractor = timestampExtractor;
        this.elements = new OrderedElements<>(timestampExtractor);
    }

    // need to synchronize to send in order
    public synchronized void collectInOrder(OutputTag<X> tag, X element) {
        elements.addInOrder(element);
        for (X e : elements.removeContiguousWith(lastRemoved)) {
            safeCollect(tag, e);
            lastRemoved = timestampExtractor.apply(e);
        }
    }
}
