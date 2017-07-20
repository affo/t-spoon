package it.polimi.affetti.tspoon.tgraph.db;

import java.io.Serializable;

/**
 * Created by affo on 14/07/17.
 */
public class ObjectVersion<T> implements Serializable {
    public final int version;
    public final T object;

    private ObjectVersion(int version, T object) {
        this.version = version;
        this.object = object;
    }

    public static <T> ObjectVersion<T> of(int version, T object) {
        return new ObjectVersion<>(version, object);
    }
}
