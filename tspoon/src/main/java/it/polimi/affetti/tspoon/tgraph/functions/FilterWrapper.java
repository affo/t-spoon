package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by affo on 28/07/17.
 */
public abstract class FilterWrapper<T> implements MapFunction<Enriched<T>, Enriched<T>> {
    @Override
    public Enriched<T> map(Enriched<T> e) throws Exception {
        T value = doFilter(e.value) ? e.value : null;
        return e.replace(value);
    }

    protected abstract boolean doFilter(T value) throws Exception;
}
