package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.OrderedElements;
import org.apache.flink.util.Preconditions;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

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
 *
 *
 * The lock/unlock methods are meant to be used by an operation before/after interacting with this object
 * The lock is especially meant for interleaving of multi-partition and single-partition updates.
 *
 * In the case of multi-partition updates, the lock must NOT be used for the entire life-span of the transaction,
 * but only for the life span of the operation to avoid conflicts with running single-partition queries.
 *
 * Single partition queries that try to lock, will both wait for the lock to be free and for "version stability"
 * (there is no version that is not committed or aborted; alternatively, every version created by multi-partition
 * transaction has been deleted/installed after an agreement has been reached).
 *
 * NOTE that single/single and multi/multi conflicts on the same key are guaranteed not to happen by construction.
 */
public class Object<T> implements Serializable {
    private transient Logger LOG;
    public static int maxNumberOfVersions = 100;
    private static boolean forceSerializableRead = false;

    public final String nameSpace, key;
    private final ObjectFunction<T> objectFunction;
    private OrderedElements<ObjectVersion<T>> versions;
    private ObjectVersion<T> lastVersion;
    private ObjectVersion<T> lastCommittedVersion;
    private boolean locked = false;
    // counter for number of versions in UNKNOWN status
    private int numberOfUnknowns = 0;

    // for external event logging
    private List<DeferredReadListener> deferredReadListeners = new LinkedList<>();

    public Object(String nameSpace, String key, ObjectFunction<T> objectFunction) {
        this.nameSpace = nameSpace;
        this.key = key;
        this.objectFunction = objectFunction;
        this.versions = new OrderedElements<>(obj -> (long) obj.version);
        this.lastCommittedVersion = initObject();
        this.lastVersion = initObject();
        this.versions.addInOrder(lastVersion);

        this.LOG = Logger.getLogger("Object<" + lastVersion.object.getClass().getSimpleName()
                + ">@" + nameSpace + "." + key);
    }

    public static void forceSerializableRead() {
        forceSerializableRead = true;
    }

    public void listenToDeferredReads(DeferredReadListener listener) {
        deferredReadListeners.add(listener);
    }

    private ObjectVersion<T> initObject() {
        ObjectVersion<T> initObject = ObjectVersion.of(this,
                0, 0, objectFunction.defaultValue());
        initObject.setStatus(ObjectVersion.Status.COMMITTED);
        return initObject;
    }

    synchronized int getVersionCount() {
        return versions.size();
    }

    private synchronized boolean isStable() {
        return numberOfUnknowns == 0;
    }

    /**
     * See class description
     * @param singlePartition if the update is single or multi partition
     */
    public void lock(boolean singlePartition) {
        try {
            if (singlePartition) {
                // single-partition queries should wait for stability of versions
                // and then lock the object to make other multi-partition wait for them
                synchronized (this) {
                    while (locked || !isStable()) {
                        wait();
                    }
                    locked = true;
                    return;
                }
            }

            // multi-partition queries should wait for single-partition ones to finish
            synchronized (this) {
                while (locked) {
                    wait();
                }
                locked = true;
            }
        } catch (InterruptedException ie) {
            LOG.error("[INTERRUPTED] Something, somewhere went horribly wrong: " + ie.getMessage());
            throw new RuntimeException("Interrupted while waiting for version stability");
        }
    }

    public synchronized void unlock() {
        locked = false;
        notifyAll();
    }

    // ------------------------ Methods for managing versions

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

    public synchronized ObjectVersion<T> addVersion(int tid, int version, T object) {
        ObjectVersion<T> objectVersion = ObjectVersion.of(this, version, tid, object);
        versions.addInOrder(objectVersion);

        if (version > lastVersion.version) {
            lastVersion = objectVersion;
        }

        numberOfUnknowns++;

        return objectVersion;
    }

    // adds and commits atomically
    public synchronized ObjectVersion<T> installVersion(int tid, int version, T object) {
        ObjectVersion<T> objectVersion = addVersion(tid, version, object);
        objectVersion.commit();
        return objectVersion;
    }

    public synchronized void signalCommit(ObjectVersion<T> objectVersion) {
        Preconditions.checkArgument(objectVersion.isCommitted(),
                "Signalled the commit of a transaction that hasn't committed");

        if (lastCommittedVersion == null || objectVersion.version > lastCommittedVersion.version) {
            lastCommittedVersion = objectVersion;
        }

        numberOfUnknowns--;

        performVersionCleanup(objectVersion.version);

        // if somebody is waiting for versions to change their status
        notifyAll();
    }

    public synchronized void signalAbort(ObjectVersion<T> objectVersion) {
        Preconditions.checkArgument(objectVersion.isAborted(),
                "Signalled the abort of a transaction that hasn't aborted");

        numberOfUnknowns--;

        performVersionCleanup(objectVersion.version);

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
            if (current.version >= version || current.isUnknown()) {
                break;
            }
            iterator.remove();
            removedCount++;
        }

        // we need to be sure to preserve at least the last committed version
        if (getVersionCount() == 0) {
            versions.addInOrder(lastCommittedVersion);
        }

        LOG.info("Version clean-up performed up to " + version);

        return removedCount;
    }

    // ------------------------ Methods for reading versions (must be called within lock/unlock calls)

    private ObjectHandler<T> createHandler(ObjectVersion<T> version) {
        return new ObjectHandler<>(version.object, version.version, version.createdBy, objectFunction::invariant);
    }

    public synchronized ObjectHandler<T> readLastVersionBefore(int timestamp) {
        return createHandler(getLastVersionBefore(timestamp));
    }

    public synchronized ObjectHandler<T> readLastAvailableVersion() {
        return createHandler(getLastAvailableVersion());
    }

    public synchronized ObjectHandler<T> readLastCommittedVersion() {
        return createHandler(lastCommittedVersion);
    }

    /**
     * If forceSerializableRead waits for every version before `version` not to be in UNKNOWN state
     * @param version
     * @return
     */
    public synchronized ObjectHandler<T> readCommittedBefore(int version) {
        if (forceSerializableRead) {
            try {
                return createHandler(serializableLastCommittedVersion(version));
            } catch (InterruptedException e) {
                throw new RuntimeException("ForceSerializableMode: interrupted while reading version "
                        + version + ": " + e.getMessage());
            }
        }

        return createHandler(simpleLastCommittedVersion(version));
    }

    private ObjectVersion<T> simpleLastCommittedVersion(int version) {
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

    private ObjectVersion<T> serializableLastCommittedVersion(int version) throws InterruptedException {
        ObjectVersion<T> lastVersionBefore;
        boolean inconsistencyPrevented = false;

        do {
            lastVersionBefore = getLastVersionBefore(version);

            while (lastVersionBefore.isUnknown()) {
                inconsistencyPrevented = true;
                LOG.info("Waiting for stable versions up to " + version + "...");
                wait();
            }

            version = lastVersionBefore.version;
        } while (!lastVersionBefore.isCommitted());

        if (inconsistencyPrevented) {
            LOG.info("Stability reached");
            notifyDeferredRead();
        }

        return lastVersionBefore;
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
