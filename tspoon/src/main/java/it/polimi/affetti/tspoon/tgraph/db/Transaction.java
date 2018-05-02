package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.state.Update;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by affo on 19/12/17.
 */
public class Transaction<V> {
    public final long tid, timestamp, watermark;
    public Vote vote = Vote.COMMIT;
    private final Set<Long> dependencies; // should be transaction ids
    private final Map<String, Object<V>> touchedObjects = new HashMap<>();
    private final Map<String, ObjectVersion<V>> objectVersions = new HashMap<>();
    private final Map<String, Operation<V>> ops = new HashMap<>();
    private final Address coordinator;
    private List<Update<V>> updates;
    private boolean readOnly;


    public Transaction(long tid, long timestamp, long watermark, Address coordinator) {
        this.tid = tid;
        this.timestamp = timestamp;
        this.watermark = watermark;
        this.coordinator = coordinator;

        this.dependencies = new HashSet<>();
        this.updates = new LinkedList<>();
    }

    public synchronized void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public synchronized boolean isReadOnly() {
        return readOnly;
    }

    public synchronized void addOperation(String key, Object<V> object, Operation<V> operation) {
        this.touchedObjects.put(key, object);
        this.ops.put(key, operation);
    }

    public synchronized void addVersion(String key, ObjectVersion<V> objectVersion) {
        objectVersions.put(key, objectVersion);
    }

    public synchronized Object<V> getObject(String key) {
        return touchedObjects.get(key);
    }

    public synchronized V getVersion(String key) {
        return objectVersions.get(key).object;
    }

    public synchronized Iterable<String> getKeys() {
        return touchedObjects.keySet();
    }

    public synchronized Operation<V> getOperation(String key) {
        return ops.get(key);
    }

    public synchronized void mergeVote(Vote vote) {
        this.vote = this.vote.merge(vote);
    }

    public synchronized Vote getVote() {
        return vote;
    }

    public synchronized Address getCoordinator() {
        return coordinator;
    }

    public synchronized void addDependency(long dependency) {
        this.dependencies.add(dependency);
    }

    public synchronized void addDependencies(Collection<Long> dependencies) {
        this.dependencies.addAll(dependencies);
    }

    public synchronized Set<Long> getDependencies() {
        return dependencies;
    }

    private List<Update<V>> calculateUpdates() {
        return objectVersions.entrySet().stream()
                .map((Map.Entry<String, ObjectVersion<V>> entry) -> Update.of(tid, entry.getKey(),
                        entry.getValue().object))
                .collect(Collectors.toList());
    }

    /**
     * I expect this to be called only upon transaction completion
     *
     * @return
     */
    public synchronized List<Update<V>> getUpdates() {
        if (!isReadOnly() && updates == null) {
            updates = calculateUpdates();
        }
        return updates;
    }

    public synchronized Iterable<Update<V>> applyChanges() {
        List<Update<V>> updates = Collections.emptyList();
        if (!isReadOnly()) {
            if (vote == Vote.COMMIT) {
                commit();
                // calculate the updates for the first time
                updates = getUpdates();
            } else {
                abort();
            }
        }

        return updates;
    }

    // NOTE that commit/abort on multiple objects is not atomic wrt external queries and internal operations
    private void commit() {
        if (vote != Vote.COMMIT) {
            throw new IllegalStateException("Cannot commit transaction with vote: " + vote);
        }

        for (ObjectVersion<V> objectVersion : objectVersions.values()) {
            objectVersion.commit();
        }
    }

    private void abort() {
        if (vote == Vote.COMMIT) {
            throw new IllegalStateException("Cannot abort a committed transaction");
        }

        for (ObjectVersion<V> objectVersion : objectVersions.values()) {
            objectVersion.abort();
        }
    }
}