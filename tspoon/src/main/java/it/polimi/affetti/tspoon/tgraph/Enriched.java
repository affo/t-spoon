package it.polimi.affetti.tspoon.tgraph;

import java.io.Serializable;

/**
 * Created by affo on 13/07/17.
 */
public class Enriched<T> implements Serializable {
    public final Metadata metadata;
    public final T value;

    public Enriched() {
        metadata = new Metadata();
        value = null;
    }

    public Enriched(Metadata meta, T value) {
        this.metadata = meta;
        this.value = value;
    }

    public static <R> Enriched<R> of(Metadata transactionContext, R value) {
        return new Enriched<>(transactionContext, value);
    }

    public <R> Enriched<R> replace(R value) {
        return Enriched.of(metadata, value);
    }

    @Override
    public String toString() {
        return "Enriched{" +
                "metadata=" + metadata +
                ", value=" + value +
                '}';
    }
}
