package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Created by affo on 28/07/17.
 */
public abstract class KeySelectorWrapper<T> implements KeySelector<Enriched<T>, Object> {
    @Override
    public Object getKey(Enriched<T> enriched) throws Exception {
        if (enriched.value == null) {
            return 0; // map every null value to same partition
        }

        return doGetKey(enriched.value);
    }

    protected abstract Object doGetKey(T value) throws Exception;
}
