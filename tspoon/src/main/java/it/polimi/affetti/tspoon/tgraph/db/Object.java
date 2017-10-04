package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.OrderedElements;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by affo on 20/07/17.
 * <p>
 * Thread-safe
 */
public class Object<T> implements Serializable {
    private OrderedElements<ObjectVersion<T>> versions;
    private ObjectVersion<T> lastVersion = initObject();
    private ObjectVersion<T> lastCommittedVersion = initObject();

    public Object() {
        this.versions = new OrderedElements<>(obj -> (long) obj.version);
    }

    private ObjectVersion<T> initObject() {
        return ObjectVersion.of(0, null);
    }

    public synchronized int getVersionCount() {
        return versions.size();
    }

    public synchronized ObjectVersion<T> getVersion(int timestamp) {
        for (ObjectVersion<T> obj : versions) {
            if (obj.version == timestamp) {
                return obj;
            }
        }

        throw new IllegalArgumentException("Version not found: " + timestamp);
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
            res = initObject();
        }
        return res;
    }

    public synchronized ObjectVersion<T> getLastAvailableVersion() {
        return lastVersion;
    }

    public synchronized Iterable<ObjectVersion<T>> getVersionsWithin(int startExclusive, int endInclusive) {
        List<ObjectVersion<T>> versions = new LinkedList<>();

        for (ObjectVersion<T> obj : this.versions) {
            if (obj.version > startExclusive) {
                if (obj.version > endInclusive) {
                    break;
                }

                versions.add(obj);
            }
        }

        return versions;
    }


    public synchronized void addVersion(ObjectVersion<T> obj) {
        versions.addInOrder(obj);

        if (obj.version > lastVersion.version) {
            lastVersion = obj;
        }
    }

    public synchronized void deleteVersion(int version) {
        ListIterator<ObjectVersion<T>> iterator = versions.iterator();

        ObjectVersion<T> previous = null;
        while (iterator.hasNext()) {
            ObjectVersion<T> current = iterator.next();
            if (current.version == version) {
                iterator.remove();
                break;
            }
            previous = current;
        }

        if (previous == null) {
            previous = initObject();
        }

        if (version == lastVersion.version) {
            lastVersion = previous;
        }
    }

    public synchronized int clearVersionsUntil(int version) {
        ListIterator<ObjectVersion<T>> iterator = versions.iterator();

        int removedCount = 0;
        while (iterator.hasNext()) {
            ObjectVersion<T> current = iterator.next();
            if (current.version >= version) {
                break;
            }
            iterator.remove();
            removedCount++;
        }

        // we need to be sure to preserve at least the last committed version
        if (getVersionCount() == 0) {
            addVersion(lastCommittedVersion);
        }

        return removedCount;
    }

    public synchronized void commitVersion(int timestamp) {
        if (timestamp > lastCommittedVersion.version) {
            lastCommittedVersion = getVersion(timestamp);
        }
    }

    public synchronized ObjectVersion<T> getLastCommittedVersion() {
        return lastCommittedVersion;
    }
}
