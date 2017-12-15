package it.polimi.affetti.tspoon.tgraph.db;

import java.io.Serializable;

/**
 * Created by affo on 14/07/17.
 */
public class ObjectHandler<T> implements Serializable {
    public T object;
    public boolean read, write;

    public ObjectHandler(T object) {
        this.object = object;
    }

    public T read() {
        read = true;
        return object;
    }

    public void write(T value) {
        write = true;
        object = value;
    }

    public ObjectVersion<T> object(int tid, int timestamp) {
        return ObjectVersion.of(timestamp, tid, object);
    }
}
