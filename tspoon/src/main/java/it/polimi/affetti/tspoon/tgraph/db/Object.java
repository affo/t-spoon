package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.OrderedElements;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by affo on 20/07/17.
 */
public class Object<T> implements Serializable {
    private OrderedElements<ObjectVersion<T>> versions;

    public Object() {
        this.versions = new OrderedElements<>(Comparator.comparingInt(obj -> obj.version));
    }

    public ObjectVersion<T> getLastVersionBefore(int tid) {
        ObjectVersion<T> res = null;
        for (ObjectVersion<T> obj : versions) {
            if (obj.version > tid) {
                break;
            }

            res = obj;
        }

        if (res == null) {
            res = ObjectVersion.of(0, null);
        }
        return res;
    }

    public void addVersion(String key, ObjectVersion<T> obj) {
        versions.addInOrder(obj);
    }

    public void deleteVersion(String key, int version) {
        versions.remove(version, obj -> obj.version);
    }
}
