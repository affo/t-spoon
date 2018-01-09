package it.polimi.affetti.tspoon.tgraph.backed;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by affo on 09/01/18.
 */
public class TransferID extends Tuple2<Integer, Long> {
    public TransferID() {
    }

    public TransferID(Integer taskID, Long incrementalID) {
        super(taskID, incrementalID);
    }

    @Override
    public String toString() {
        return f0 + "." + f1;
    }
}
