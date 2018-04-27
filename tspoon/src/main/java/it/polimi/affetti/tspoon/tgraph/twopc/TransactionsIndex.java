package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedElements;
import it.polimi.affetti.tspoon.common.TimestampUtils;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
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
    private long currentLogicalTimestamp;
    private Tuple2<Long, Long> watermark;
    protected final OrderedElements<Tuple2<Long, Long>> timestamps = new OrderedElements<>(t -> t.f0);
    // tid -> tContext
    protected Map<Long, LocalTransactionContext> executions = new HashMap<>();
    // realTs <-> logicTS
    private Map<Long, Long> realLogicalTSMapping = new HashMap<>();

    private final int sourceID;

    public TransactionsIndex(long startingPoint, int sourceParallelism, int sourceID) {
        this.tid = startingPoint;
        this.currentLogicalTimestamp = startingPoint;
        this.watermark = Tuple2.of(startingPoint, 0L);

        this.sourceID = sourceID;
        TimestampUtils.init(sourceParallelism);
    }

    public long getCurrentTid() {
        return tid;
    }

    public long getCurrentWatermark() {
        return watermark.f1;
    }

    public long updateWatermark(long timestamp, Vote vote) {
        timestamps.addInOrder(Tuple2.of(realLogicalTSMapping.get(timestamp), timestamp));
        List<Tuple2<Long, Long>> removed = timestamps.removeContiguousWith(watermark.f0);

        if (!removed.isEmpty()) {
            watermark = Collections.max(removed, Comparator.comparingLong(o -> o.f1));
        }

        return watermark.f1;
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
        currentLogicalTimestamp++;
        long realTS = TimestampUtils.getTimestamp(sourceID);
        realLogicalTSMapping.put(realTS, currentLogicalTimestamp);

        LocalTransactionContext localTransactionContext = new LocalTransactionContext();
        localTransactionContext.tid = tid;
        localTransactionContext.timestamp = realTS;
        localTransactionContext.element = element;
        executions.put(tid, localTransactionContext);
        return localTransactionContext;
    }

    public void deleteTransaction(long tid) {
        long realTS = executions.remove(tid).timestamp;
        realLogicalTSMapping.remove(realTS);
    }

    public class LocalTransactionContext {
        public long tid;
        public long timestamp;
        public T element;
    }
}
