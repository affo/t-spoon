package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by affo on 28/07/17.
 */
public abstract class MapWrapper<I, O> implements MapFunction<Enriched<I>, Enriched<O>> {
    @Override
    public Enriched<O> map(Enriched<I> e) throws Exception {
        return e.replace(doMap(e.value));
    }

    public abstract O doMap(I e) throws Exception;
}
