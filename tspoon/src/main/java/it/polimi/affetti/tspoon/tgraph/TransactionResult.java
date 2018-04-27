package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Created by affo on 26/07/17.
 */
public class TransactionResult extends Tuple5<Long, Long, Object, Vote, Updates> {
    public TransactionResult() {
    }

    public TransactionResult(Long tid, Long timestamp, Object originalRecord, Vote vote, Updates updates) {
        super(tid, timestamp, originalRecord, vote, updates);
    }
}
