package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 04/08/17.
 */
public abstract class TransactionsIndex<T> implements Serializable {
    private int uniqueID = 0;
    // I need a timestamp because a transaction can possibly be executed
    // more than once. However, I need to keep it separate from the transaction ID
    // (i.e. not overwrite it), because I want to track the dependencies among
    // transactions. I use the timestamp to check on the state operators if a
    // transaction has to be replayed or not; on the other hand, I use the transaction ID
    // to track the dependencies among transactions (a dependency is specified only if the
    // conflicting transaction has a timestamp greater than the last version saved).
    private int currentTimestamp = 0;
    private int watermark = 0;
    protected final OrderedTimestamps timestamps = new OrderedTimestamps();
    // tid -> tContext
    protected Map<Integer, LocalTransactionContext> executions = new HashMap<>();

    public int getCurrentWatermark() {
        return watermark;
    }

    public int updateWatermark(int timestamp, Vote vote) {
        timestamps.addInOrder(timestamp);
        List<Integer> removed = timestamps.removeContiguousWith(watermark);

        if (!removed.isEmpty()) {
            watermark = Collections.max(removed);
        }

        return watermark;
    }

    public LocalTransactionContext getTransaction(int tid) {
        return executions.get(tid);
    }

    public LocalTransactionContext getTransactionByTimestamp(int timestamp) {
        return getTransaction(getTransactionId(timestamp));
    }

    protected abstract Integer getTransactionId(int timestamp);

    public LocalTransactionContext newTransaction(T element) {
        uniqueID++;
        return this.newTransaction(element, uniqueID);
    }

    public LocalTransactionContext newTransaction(T element, int tid) {
        currentTimestamp++;
        LocalTransactionContext localTransactionContext = new LocalTransactionContext();
        localTransactionContext.tid = tid;
        localTransactionContext.timestamp = currentTimestamp;
        localTransactionContext.element = element;
        executions.put(tid, localTransactionContext);
        return localTransactionContext;
    }

    public void deleteTransaction(int tid) {
        executions.remove(tid);
    }

    public class LocalTransactionContext {
        public int tid;
        public int timestamp;
        public Vote vote;
        public int replayCause;
        public T element;
    }
}
