package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by affo on 26/07/17.
 */
public class TransactionResult<T> extends Tuple3<Integer, Vote, T> {
    public TransactionResult() {
    }

    public TransactionResult(Integer tid, Vote vote, T value) {
        super(tid, vote, value);
    }
}
