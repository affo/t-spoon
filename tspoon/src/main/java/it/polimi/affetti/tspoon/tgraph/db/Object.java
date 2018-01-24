package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.OrderedElements;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

/**
 * Created by affo on 20/07/17.
 * <p>
 * Thread-safe class.
 *
 * By forcing serializability (`forceSerializableRead`) a call to `readCommittedBefore` ensures
 * that every registered version before the one passed has been either committed to abort;
 * i.e., the corresponding transaction has reached global COMMIT or ABORT.
 *
 * WHY: When using the ASYNCHRONOUS protocol, at >=PL2 isolation level, the sink ACKs the notification of transaction
 * completion to the coordinator and the states in parallel. This could cause new transactions to hit keys
 * (in the state operator) while the aborting transactions emitted before haven't yet removed their versions.
 * This could lead to downgrade the isolation level to READ-UNCOMMITTED.
 * At PL2 level it is enough to return the last available committed version. At PL3 we need to get a consistent
 * snapshot of the state.
 *
 * `readCommittedBefore` provides the logic to wait for the global termination of every version before
 * the requested watermark.
 *
 * NOTE that if some transaction T reads an object with a certain watermark W, this means that every transaction with timestamp
 * <= W has completed. If T hits an object and some version <= W has not yet been removed/installed it
 * is necessary to wait for their completion. At this point, it is impossible that a transaction with a smaller timestamp
 * than W has not yet hit the objects. If so, the watermark wouldn't have been W, but some V < W.
 *
 * NOTE we need to force serializable reads for:
 *  - both read-only and update transactions in the case of OPTIMISTIC strategy, ASYNCHRONOUS protocol and isolation >= PL3.
 *  - only for read-only transactions when PESSIMISTIC; updates use the locking mechanism, indeed, and read
 *  the last available committed version.
 */
public class Object<T> implements Serializable {
    public static int maxNumberOfVersions = 100;
    private static boolean forceSerializableRead = false;

    private final ObjectFunction<T> objectFunction;
    private OrderedElements<ObjectVersion<T>> versions;
    private ObjectVersion<T> lastVersion;
    private ObjectVersion<T> lastCommittedVersion;

    // for external event logging
    private List<DeferredReadListener> deferredReadListeners = new LinkedList<>();

    public Object(ObjectFunction<T> objectFunction) {
        this.objectFunction = objectFunction;
        this.versions = new OrderedElements<>(obj -> (long) obj.version);
        this.lastCommittedVersion = initObject();
        this.lastVersion = initObject();
    }

    public static void forceSerializableRead() {
        forceSerializableRead = true;
    }

    public synchronized void listenToDeferredReads(DeferredReadListener listener) {
        deferredReadListeners.add(listener);
    }

    private ObjectVersion<T> initObject() {
        ObjectVersion<T> initObject = ObjectVersion.of(
                0, 0, objectFunction.defaultValue(), objectFunction);
        initObject.commit();
        return initObject;
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

        throw new IllegalArgumentException("Version " + timestamp + " not found for Object " + this);
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

    public synchronized Iterable<ObjectVersion<T>> getVersionsAfter(int startExclusive) {
        return getVersionsWithin(startExclusive, Integer.MAX_VALUE);
    }

    /**
     * WARNING: iterates over every available version
     *
     * @param threshold
     * @return the versions younger (with a bigger createdBy id) threshold.
     * No guarantees on the createdBy id ordering wrt the timestamp.
     * The versions will be in timestamp order.
     */
    public synchronized Iterable<ObjectVersion<T>> getVersionsByNewerTransactions(int threshold) {
        List<ObjectVersion<T>> versions = new LinkedList<>();

        for (ObjectVersion<T> obj : this.versions) {
            if (obj.createdBy > threshold) {
                versions.add(obj);
            }
        }

        return versions;
    }

    /**
     * @param predicate
     * @return true if there is any version matching the predicate.
     */
    public synchronized boolean anyVersionMatch(Predicate<ObjectVersion<T>> predicate) {
        for (ObjectVersion<T> obj : this.versions) {
            if (predicate.test(obj)) {
                return true;
            }
        }

        return false;
    }

    public synchronized boolean noneVersionMatch(Predicate<ObjectVersion<T>> predicate) {
        for (ObjectVersion<T> obj : this.versions) {
            if (predicate.test(obj)) {
                return false;
            }
        }

        return true;
    }


    public synchronized ObjectVersion<T> addVersion(int tid, int version, T object) {
        ObjectVersion<T> objectVersion = ObjectVersion.of(version, tid, object, objectFunction);
        versions.addInOrder(objectVersion);

        if (version > lastVersion.version) {
            lastVersion = objectVersion;
        }

        return objectVersion;
    }

    public synchronized void commitVersion(int timestamp) {
        ObjectVersion<T> version = getVersion(timestamp);
        // change version status
        version.commit();

        if (timestamp > lastCommittedVersion.version) {
            lastCommittedVersion = version;
        }

        // if somebody is waiting for versions to change their status
        notifyAll();
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

        // if somebody is waiting for versions to change their status
        notifyAll();
    }

    public synchronized int performVersionCleanup(int version) {
        if (getVersionCount() <= maxNumberOfVersions) {
            return 0;
        }

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
            versions.addInOrder(lastCommittedVersion);
        }

        return removedCount;
    }

    public synchronized ObjectVersion<T> getLastCommittedVersion() {
        return lastCommittedVersion;
    }

    /**
     * If forceSerializableRead waits for every version before `version` not to be in UNKNOWN state
     * @param version
     * @return
     */
    public synchronized ObjectVersion<T> readCommittedBefore(int version) {
        if (forceSerializableRead) {
            try {
                return serializableLastCommittedVersion(version);
            } catch (InterruptedException e) {
                throw new RuntimeException("ForceSerializableMode: interrupted while reading version "
                        + version + ": " + e.getMessage());
            }
        }

        return simpleLastCommittedVersion(version);
    }

    private synchronized ObjectVersion<T> simpleLastCommittedVersion(int version) {
        ObjectVersion<T> lastCommittedBefore = null;

        for (ObjectVersion<T> objectVersion : versions) {
            if (objectVersion.version > version) {
                break;
            }

            if (objectVersion.isCommitted()) {
                lastCommittedBefore = objectVersion;
            }
        }

        if (lastCommittedBefore == null) {
            lastCommittedBefore = initObject();
        }

        return lastCommittedBefore;
    }

    private synchronized ObjectVersion<T> serializableLastCommittedVersion(int version) throws InterruptedException {
        ObjectVersion<T> lastCommittedBefore;
        boolean inconsistencyPrevented = false;
        do {
            lastCommittedBefore = getLastVersionBefore(version);
            if (!lastCommittedBefore.isCommitted()) {
                inconsistencyPrevented = true;
                wait();
            }
        } while (!lastCommittedBefore.isCommitted());

        if (inconsistencyPrevented) {
            notifyDeferredRead();
        }

        return lastCommittedBefore;
    }

    private void notifyDeferredRead() {
        for (DeferredReadListener listener : deferredReadListeners) {
            listener.onDeferredExecution();
        }
    }

    public interface DeferredReadListener {
        void onDeferredExecution();
    }
}
