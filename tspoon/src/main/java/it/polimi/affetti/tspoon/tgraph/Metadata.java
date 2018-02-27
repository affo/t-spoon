package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.Address;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by affo on 13/07/17.
 */
public class Metadata implements Serializable {
    // The original record, needs to be casted for proper use
    // TODO parametrize Metadata on the originalRecord
    public Object originalRecord;
    public int tGraphID;
    public BatchID batchID;
    public int tid, timestamp;
    public HashSet<Address> cohorts = new HashSet<>();
    public Address coordinator;
    public Vote vote = Vote.COMMIT;
    public int watermark = 0;
    public HashSet<Integer> dependencyTracking = new HashSet<>();
    public Updates updates = new Updates();

    public Metadata() {
    }

    public Metadata(int tid) {
        this(0, tid);
    }

    public Metadata(int tGraphID, int tid) {
        this.tGraphID = tGraphID;
        this.batchID = new BatchID(tid);
        this.tid = tid;
        this.timestamp = tid;
    }

    public Metadata(int tid, Vote vote, int watermark) {
        this(tid);
        this.vote = vote;
        this.watermark = watermark;
    }

    public Metadata deepClone(BatchID bid) {
        Metadata cloned = new Metadata();
        cloned.tGraphID = tGraphID;
        cloned.batchID = bid;
        cloned.tid = tid;
        cloned.timestamp = timestamp;
        cloned.cohorts = new HashSet<>(cohorts);
        cloned.coordinator = coordinator;
        cloned.vote = vote;
        cloned.watermark = watermark;
        cloned.dependencyTracking = new HashSet<>(dependencyTracking);
        cloned.updates = updates.clone();
        cloned.originalRecord = originalRecord;
        return cloned;
    }

    public void addCohort(Address cohortAddress) {
        cohorts.add(cohortAddress);
    }

    public void addUpdate(String namespace, String key, Object update) {
        this.updates.addUpdate(namespace, key, update);
    }

    public void mergeUpdates(Updates updates) {
        this.updates.merge(updates);
    }

    public Iterator<Address> cohorts() {
        return cohorts.iterator();
    }

    // invoke it at most once per function call
    public Iterable<Metadata> newStep(int batchSize) {
        batchID.consolidate();
        List<BatchID> batchIDS = this.batchID.addStep(batchSize);
        return batchIDS.stream()
                .map(bid -> this.deepClone(bid.clone()))
                .collect(Collectors.toList());
    }

    public int getLastStepBatchSize() {
        int size = 0;
        for (Tuple2<Integer, Integer> offsetSize : batchID) {
            size = offsetSize.f1;
        }
        return size;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "bid=" + batchID +
                ", timestamp=" + timestamp +
                ", cohorts=" + cohorts +
                ", coordinator=" + coordinator +
                ", vote=" + vote +
                ", watermark=" + watermark +
                ", dependencyTracking=" + dependencyTracking +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metadata metadata = (Metadata) o;

        return timestamp == metadata.timestamp;
    }

    @Override
    public int hashCode() {
        return timestamp;
    }
}
