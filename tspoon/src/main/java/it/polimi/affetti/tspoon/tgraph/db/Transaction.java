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
    public final int tid, timestamp, watermark;
    public Vote vote = Vote.COMMIT;
    private final Set<Integer> dependencies;
    private final Map<String, Object<V>> touchedObjects = new HashMap<>();
    private final Map<String, ObjectVersion<V>> objectVersions = new HashMap<>();
    private final Map<String, Operation<V>> ops = new HashMap<>();
    private final Address coordinator;
    private List<Update<V>> updates;
    private boolean readOnly;


    public Transaction(int tid, int timestamp, int watermark, Address coordinator) {
        this.tid = tid;
        this.timestamp = timestamp;
        this.watermark = watermark;
        this.coordinator = coordinator;

        this.dependencies = new HashSet<>();
        this.updates = new LinkedList<>();
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void addOperation(String key, Object<V> object, Operation<V> operation) {
        this.touchedObjects.put(key, object);
        this.ops.put(key, operation);
    }

    public void addVersion(String key, ObjectVersion<V> objectVersion) {
        objectVersions.put(key, objectVersion);
    }

    Object<V> getObject(String key) {
        return touchedObjects.get(key);
    }

    Iterable<String> getKeys() {
        return touchedObjects.keySet();
    }

    Operation<V> getOperation(String key) {
        return ops.get(key);
    }

    public void mergeVote(Vote vote) {
        this.vote = this.vote.merge(vote);
    }

    public Vote getVote() {
        return vote;
    }

    public Address getCoordinator() {
        return coordinator;
    }

    public void addDependency(int dependency) {
        this.dependencies.add(dependency);
    }

    public void addDependencies(Collection<Integer> dependencies) {
        this.dependencies.addAll(dependencies);
    }

    public Set<Integer> getDependencies() {
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
    public List<Update<V>> getUpdates() {
        if (!isReadOnly() && updates == null) {
            updates = calculateUpdates();
        }
        return updates;
    }

    public Iterable<Update<V>> applyChanges() {
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
    public void commit() {
        if (vote != Vote.COMMIT) {
            throw new IllegalStateException("Cannot commit transaction with vote: " + vote);
        }

        for (Map.Entry<String, ObjectVersion<V>> entry : objectVersions.entrySet()) {
            String key = entry.getKey();
            int version = entry.getValue().version;
            Object<V> object = touchedObjects.get(key);
            object.commitVersion(version);
            object.performVersionCleanup(version);
        }
    }

    public void abort() {
        if (vote == Vote.COMMIT) {
            throw new IllegalStateException("Cannot abort a committed transaction");
        }

        for (Map.Entry<String, ObjectVersion<V>> entry : objectVersions.entrySet()) {
            String key = entry.getKey();
            int version = entry.getValue().version;
            Object<V> object = touchedObjects.get(key);
            object.deleteVersion(version);
        }
    }
}