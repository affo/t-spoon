package it.polimi.affetti.tspoon.test;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;

/**
 * Created by affo on 28/07/17.
 */
public class CollectionSource<T> implements SourceFunction<T> {
    Collection<T> elements;

    public CollectionSource(Collection<T> elements) {
        this.elements = elements;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        for (T element : elements) {
            sourceContext.collect(element);
        }
    }

    @Override
    public void cancel() {

    }
}
