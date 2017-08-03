package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.OrderedElements;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by affo on 20/07/17.
 * <p>
 * Thread-safe
 */
public class Object<T> implements Serializable {
    private OrderedElements<ObjectVersion<T>> versions;
    private int lastCommittedVersion;

    public Object() {
        this.versions = new OrderedElements<>(Comparator.comparingInt(obj -> obj.version));
    }

    public synchronized ObjectVersion<T> getLastVersionBefore(int timestamp) {
        ObjectVersion<T> res = null;
        for (ObjectVersion<T> obj : versions) {
            if (obj.version > timestamp) {
                break;
            }

            res = obj;
        }

        if (res == null) {
            res = ObjectVersion.of(0, null);
        }
        return res;
    }

    public synchronized void addVersion(ObjectVersion<T> obj) {
        versions.addInOrder(obj);
    }

    public synchronized void deleteVersion(int version) {
        versions.remove(version, obj -> obj.version);
    }

    public synchronized void commit(int version) {
        if (version > lastCommittedVersion) {
            lastCommittedVersion = version;
        }
    }

    public synchronized ObjectVersion<T> getLastCommittedVersion() {
        ObjectVersion<T> object = null;

        for (ObjectVersion<T> version : versions) {
            if (version.version == lastCommittedVersion) {
                object = version;
            }
        }

        return object;
    }
}
