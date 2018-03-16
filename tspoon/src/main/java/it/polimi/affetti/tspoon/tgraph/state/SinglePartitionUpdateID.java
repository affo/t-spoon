package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.evaluation.UniquelyRepresentableForTracking;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by affo on 09/01/18.
 */
public class SinglePartitionUpdateID extends Tuple2<Integer, Long> implements UniquelyRepresentableForTracking {
    public SinglePartitionUpdateID() {
    }

    public SinglePartitionUpdateID(Integer taskID, Long incrementalID) {
        super(taskID, incrementalID);
    }

    @Override
    public String toString() {
        return f0 + "." + f1;
    }

    @Override
    public String getUniqueRepresentation() {
        return toString();
    }
}
