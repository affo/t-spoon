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
    public int replayCause = -1;

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

    public void multiplyBatchSize(int factor) {
        batchSize *= factor;
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
                ", replayCause=" + replayCause +
                '}';
    }
}
