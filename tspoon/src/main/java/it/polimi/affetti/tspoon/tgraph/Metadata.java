package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.Address;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by affo on 13/07/17.
 */
public class Metadata implements Serializable {
    public int tid;
    public int timestamp;
    public Set<Address> cohorts = new HashSet<>();
    public Address coordinator;
    public Vote vote = Vote.COMMIT;
    public int offset = 1;
    public int batchSize = 1;
    public int watermark = 0;
    public Set<Integer> dependencyTracking = new HashSet<>();

    public Metadata() {
    }

    public Metadata(int tid) {
        this.tid = tid;
        this.timestamp = tid;
    }

    public Metadata(int tid, Vote vote, int watermark) {
        this.tid = tid;
        this.timestamp = tid;
        this.vote = vote;
        this.watermark = watermark;
    }

    public void addCohort(Address cohortAddress) {
        cohorts.add(cohortAddress);
    }

    public Iterator<Address> cohorts() {
        return cohorts.iterator();
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isCollectable() {
        return vote == Vote.COMMIT;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "tid=" + tid +
                ", timestamp=" + timestamp +
                ", cohorts=" + cohorts +
                ", coordinator=" + coordinator +
                ", vote=" + vote +
                ", offset=" + offset +
                ", batchSize=" + batchSize +
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
