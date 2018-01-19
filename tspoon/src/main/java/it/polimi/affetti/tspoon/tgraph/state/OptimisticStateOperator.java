package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.OptimisticTransactionExecutor;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;
import it.polimi.affetti.tspoon.tgraph.twopc.TRuntimeContext;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by affo on 25/07/17.
 */
public class OptimisticStateOperator<T, V> extends StateOperator<T, V> {
    private transient OptimisticTransactionExecutor transactionExecutor;

    // stats
    private IntCounter replays = new IntCounter();
    private Map<String, Long> hitTimestamps = new HashMap<>();
    private MetricAccumulator hitRate = new MetricAccumulator();
    private MetricAccumulator inputRate = new MetricAccumulator();
    private Long lastTS;

    public OptimisticStateOperator(
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            TRuntimeContext tRuntimeContext) {
        super(nameSpace, stateFunction, updatesTag, tRuntimeContext);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.transactionExecutor = new OptimisticTransactionExecutor(
                tRuntimeContext.getIsolationLevel(),
                tRuntimeContext.isDependencyTrackingEnabled()
        );

        getRuntimeContext().addAccumulator("replays-at-state", replays);
        getRuntimeContext().addAccumulator("hit-rate", hitRate);
        getRuntimeContext().addAccumulator("input-rate", inputRate);
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) {
        transactionExecutor.executeOperation(key, transaction);

        record.metadata.dependencyTracking = new HashSet<>(transaction.getDependencies());
        record.metadata.vote = transaction.vote;
        collector.safeCollect(record);

        if (transaction.vote == Vote.REPLAY) {
            replays.add(1);
        }

        if (lastTS == null) {
            lastTS = System.nanoTime();
        }

        long now = System.nanoTime();
        inputRate.add(Math.pow(10, 9) / (double) (now - lastTS));
        lastTS = now;

        Long lastTimestampForThisKey = hitTimestamps.get(key);
        if (lastTimestampForThisKey != null) {
            hitRate.add(Math.pow(10, 9) / (double) (now - lastTimestampForThisKey));
        }
        hitTimestamps.put(key, now);
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        // does nothing
    }
}
