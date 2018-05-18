package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedElements;
import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;
import java.sql.Time;
import java.util.*;

/**
 * Created by affo on 04/08/17.
 */
public abstract class TransactionsIndex<T> implements Serializable {
    private long tid;
    // I need a timestamp because a transaction can possibly be executed
    // more than once. However, I need to keep it separate from the transaction ID
    // (i.e. not overwrite it), because I want to track the dependencies among
    // transactions. I use the timestamp to check on the state operators if a
    // transaction has to be replayed or not; on the other hand, I use the transaction ID
    // to track the dependencies among transactions (a dependency is specified only if the
    // conflicting transaction has a timestamp greater than the last version saved).
    private TimestampGenerator timestampGenerator;
    private long watermark;
    protected final OrderedElements<Long> timestamps = new OrderedElements<>(l -> l);
    // tid -> tContext
    protected Map<Long, LocalTransactionContext> executions = new HashMap<>();

    public TransactionsIndex(long startingTid, TimestampGenerator timestampGenerator) {
        this.tid = startingTid;
        this.timestampGenerator = timestampGenerator;
        this.watermark = timestampGenerator.toLogical(startingTid);
    }

    public long getCurrentTid() {
        return tid;
    }

    public long getCurrentWatermark() {
        return timestampGenerator.toReal(watermark);
    }

    public long updateWatermark(long timestamp, Vote vote) {
        timestamps.addInOrder(timestampGenerator.toLogical(timestamp));
        List<Long> removed = timestamps.removeContiguousWith(watermark);

        if (!removed.isEmpty()) {
            watermark = Collections.max(removed);
        }

        return timestampGenerator.toReal(watermark);
    }

    public LocalTransactionContext getTransaction(long tid) {
        return executions.get(tid);
    }

    public boolean isTransactionRunning(long tid) {
        return executions.containsKey(tid);
    }

    /**
     * Use it for debug
     */
    public int getNumberOfRunningTransactions() {
        return executions.size();
    }

    /**
     * Use it for debug
     */
    public Set<Long> getRunningTids() {
        return executions.keySet();
    }

    public LocalTransactionContext getTransactionByTimestamp(long timestamp) {
        Long tid = getTransactionId(timestamp);
        if (tid == null) {
            return null;
        }
        return getTransaction(tid);
    }

    protected abstract Long getTransactionId(long timestamp);

    public LocalTransactionContext newTransaction(T element) {
        tid++;
        return this.newTransaction(element, tid);
    }

    public LocalTransactionContext newTransaction(T element, long tid) {
        long timestamp = timestampGenerator.nextTimestamp();
        LocalTransactionContext localTransactionContext = new LocalTransactionContext();
        localTransactionContext.tid = tid;
        localTransactionContext.timestamp = timestamp;
        localTransactionContext.element = element;
        executions.put(tid, localTransactionContext);
        return localTransactionContext;
    }

    public void deleteTransaction(long tid) {
        executions.remove(tid);
    }

    public class LocalTransactionContext {
        public long tid;
        public long timestamp;
        public T element;
    }
}
