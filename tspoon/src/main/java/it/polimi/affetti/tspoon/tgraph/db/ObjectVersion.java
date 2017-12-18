package it.polimi.affetti.tspoon.tgraph.db;

import java.io.Serializable;

/**
 * Created by affo on 14/07/17.
 */
public class ObjectVersion<T> implements Serializable {
    public final int version;
    public final int createdBy;
    public final T object;
    private final ObjectFunction<T> objectFunction;

    private ObjectVersion(int version, int createdBy, T object, ObjectFunction<T> objectFunction) {
        this.version = version;
        this.createdBy = createdBy;
        this.object = object;
        this.objectFunction = objectFunction;
    }

    public static <T> ObjectVersion<T> of(int version, int createdBy, T object, ObjectFunction<T> objectFunction) {
        if (object == null) {
            throw new NullPointerException();
        }

        return new ObjectVersion<>(version, createdBy, object, objectFunction);
    }

    public static <T> ObjectVersion<T> of(int version, int createdBy, ObjectFunction<T> objectFunction) {
        return new ObjectVersion<>(version, createdBy, objectFunction.defaultValue(), objectFunction);
    }

    public ObjectHandler<T> createHandler() {
        return new ObjectHandler<>(objectFunction.copyValue(object), objectFunction::invariant);
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
