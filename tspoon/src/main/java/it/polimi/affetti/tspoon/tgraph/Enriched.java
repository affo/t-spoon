package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by affo on 13/07/17.
 */
public class Enriched<T> extends Tuple2<TransactionContext, T> {
    public Enriched() {
    }

    public Enriched(TransactionContext meta, T value) {
        super(meta, value);
    }

    public TransactionContext tContext() {
        return f0;
    }

    public T value() {
        return f1;
    }

    public static <R> Enriched<R> of(TransactionContext transactionContext, R value) {
        return new Enriched<>(transactionContext, value);
    }

    public <R> Enriched<R> replace(R value) {
        return Enriched.of(tContext(), value);
    }
}
