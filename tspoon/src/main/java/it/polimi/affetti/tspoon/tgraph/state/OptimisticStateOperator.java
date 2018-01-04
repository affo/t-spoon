package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.db.OptimisticTransactionExecutor;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;
import it.polimi.affetti.tspoon.tgraph.twopc.TRuntimeContext;
import org.apache.flink.util.OutputTag;

import java.util.HashSet;

/**
 * Created by affo on 25/07/17.
 */
public class OptimisticStateOperator<T, V> extends StateOperator<T, V> {
    private transient OptimisticTransactionExecutor transactionExecutor;

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
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) {
        transactionExecutor.executeOperation(key, transaction);

        record.metadata.dependencyTracking = new HashSet<>(transaction.getDependencies());
        record.metadata.vote = transaction.vote;
        collector.safeCollect(record);
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        // does nothing
    }
}
