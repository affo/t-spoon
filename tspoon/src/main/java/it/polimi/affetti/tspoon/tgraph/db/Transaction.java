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

    public void addDependencies(Collection<Integer> dependencies) {
        this.dependencies.addAll(dependencies);
    }

    public Set<Integer> getDependencies() {
        return dependencies;
    }

    private List<Update<V>> calculateUpdates() {
        return touchedObjects.entrySet().stream()
                .map((Map.Entry<String, Object<V>> entry) -> Update.of(tid, entry.getKey(),
                        entry.getValue().getVersion(timestamp).object))
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
        // NOTE that commit/abort on multiple objects is not atomic wrt external queries and internal operations
        if (!isReadOnly()) {
            if (vote == Vote.COMMIT) {
                for (Object<V> object : touchedObjects.values()) {
                    object.commitVersion(timestamp);
                    object.performVersionCleanup(timestamp);
                }

                updates = getUpdates();
            } else {
                for (Object<V> object : touchedObjects.values()) {
                    object.deleteVersion(timestamp);
                }
            }
        }

        return updates;
    }
}