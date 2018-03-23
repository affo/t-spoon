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
    private final Object<T> parent;

    private ObjectVersion(Object<T> parent, int version, int createdBy, T object) {
        this.parent = parent;
        this.version = version;
        this.createdBy = createdBy;
        this.object = object;
    }

    public static <T> ObjectVersion<T> of(Object<T> parent, int version, int createdBy, T object) {
        if (object == null) {
            throw new NullPointerException();
        }

        return new ObjectVersion<>(parent, version, createdBy, object);
    }

    public static <T> ObjectVersion<T> of(Object<T> parent, int version, int createdBy, ObjectFunction<T> objectFunction) {
        return new ObjectVersion<>(parent, version, createdBy, objectFunction.defaultValue());
    }

    public void commit() {
        setStatus(Status.COMMITTED);
        parent.signalCommit(this);
    }

    public void abort() {
        setStatus(Status.ABORTED);
        parent.signalAbort(this);
    }

    public boolean isCommitted() {
        return status == Status.COMMITTED;
    }

    public boolean isAborted() {
        return status == Status.ABORTED;
    }

    public boolean isUnknown() {
        return !isCommitted() && !isAborted();
    }

    public Status getStatus() {
        return status;
    }

    /**
     * NOTE that setStatus(COMMITTED) is not the same as objectVersion.commit()
     * The second one notifies also the parent and triggers its "signal" callback.
     * @param status
     */
    public void setStatus(Status status) {
        this.status = status;
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
        COMMITTED, ABORTED, UNKNOWN
    }
}
