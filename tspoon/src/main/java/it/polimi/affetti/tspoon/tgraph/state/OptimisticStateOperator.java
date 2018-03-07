package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.OptimisticTransactionExecutor;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;
import it.polimi.affetti.tspoon.tgraph.twopc.TRuntimeContext;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

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
            int tGraphID,
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            KeySelector<T, String> ks,
            TRuntimeContext tRuntimeContext) {
        super(tGraphID, nameSpace, stateFunction, updatesTag, ks, tRuntimeContext);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.transactionExecutor = new OptimisticTransactionExecutor(
                tRuntimeContext.getIsolationLevel(),
                tRuntimeContext.isDependencyTrackingEnabled(),
                tRuntimeContext.needWaitOnRead()
        );

        getRuntimeContext().addAccumulator("replays-at-state", replays);
        getRuntimeContext().addAccumulator("hit-rate", hitRate);
        getRuntimeContext().addAccumulator("input-rate", inputRate);
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionExecutor.close();
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) {
        Consumer<Void> andThen = aVoid -> {
            record.metadata.dependencyTracking = new HashSet<>(transaction.getDependencies());
            record.metadata.vote = transaction.vote;
            decorateRecordWithUpdates(key, record, transaction);
            collector.safeCollect(record);

            // ------------ updates stats

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
        };

        transactionExecutor.executeOperation(key, transaction, andThen);
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        // does nothing
    }
}
