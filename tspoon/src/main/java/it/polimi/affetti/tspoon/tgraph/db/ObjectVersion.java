package it.polimi.affetti.tspoon.tgraph.db;

import java.io.Serializable;

/**
 * Created by affo on 14/07/17.
 */
public class ObjectVersion<T> implements Serializable {
    public final int version;
    public final int createdBy;
    public final T object;

    private ObjectVersion(int version, int createdBy, T object) {
        this.version = version;
        this.createdBy = createdBy;
        this.object = object;
    }

    public static <T> ObjectVersion<T> of(int version, int createdBy, T object) {
        return new ObjectVersion<>(version, createdBy, object);
    }

    @Override
    public String toString() {
        return "ObjectVersion{" +
                "version=" + version +
                ", createdBy=" + createdBy +
                ", object=" + object +
                '}';
    }
}
