package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 20/07/17.
 */
public class TwoPCData {
    public int tid;
    public List<Address> cohorts = new LinkedList<>();
    public Address coordinator;
    public Vote vote = Vote.COMMIT;
    public int batchSize;

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
        return "TContext{" +
                "tid=" + tid +
                ", cohorts=" + cohorts +
                ", coordinator=" + coordinator +
                ", vote=" + vote +
                ", batchSize=" + batchSize +
                '}';
    }
}
