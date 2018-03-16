package it.polimi.affetti.tspoon.tgraph.db;

import java.io.Serializable;

/**
 * Created by affo on 14/07/17.
 */
public class ObjectVersion<T> implements Serializable {
    public final int version;
    public final int createdBy;
    private Status status = Status.UNKNOWN;
    public final T object;

    private ObjectVersion(int version, int createdBy, T object) {
        this.version = version;
        this.createdBy = createdBy;
        this.object = object;
    }

    public static <T> ObjectVersion<T> of(int version, int createdBy, T object) {
        if (object == null) {
            throw new NullPointerException();
        }

        return new ObjectVersion<>(version, createdBy, object);
    }

    public static <T> ObjectVersion<T> of(int version, int createdBy, ObjectFunction<T> objectFunction) {
        return new ObjectVersion<>(version, createdBy, objectFunction.defaultValue());
    }

    public void commit() {
        this.status = Status.COMMITTED;
    }

    public boolean isCommitted() {
        return status == Status.COMMITTED;
    }

    @Override
    public String toString() {
        return "ObjectVersion{" +
                "version=" + version +
                ", createdBy=" + createdBy +
                ", object=" + object +
                '}';
    }

    public enum Status {
        COMMITTED, UNKNOWN
    }
}
