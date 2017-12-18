package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.db.PessimisticTransactionExecutor;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;
import it.polimi.affetti.tspoon.tgraph.twopc.TRuntimeContext;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 10/11/17.
 * <p>
 * NOTE that implementing reads or write locks within a single state operator is useless because requires pre-processing
 * of the transaction itself. When a new transaction comes, if the key is either read or write locked, it waits for it to become
 * free. However we store if a transaction was read-only because this information can be used by the external queries
 * (that are read-only by definition).
 * <p>
 * This implies that, excluding external queries, internal transactions are inherently PL3 or PL4 isolated.
 */
public class PessimisticStateOperator<T, V> extends StateOperator<T, V> {
    private transient PessimisticTransactionExecutor transactionExecutor;
    // settings
    private boolean deadlockEnabled;
    private long deadlockTimeout;

    public PessimisticStateOperator(
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            TRuntimeContext tRuntimeContext) {
        super(nameSpace, stateFunction, updatesTag, tRuntimeContext);
    }

    public void enableDeadlockDetection(long deadlockTimeout) {
        this.deadlockEnabled = true;
        this.deadlockTimeout = deadlockTimeout;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (deadlockEnabled) {
            transactionExecutor = new PessimisticTransactionExecutor(1, deadlockTimeout);
        } else {
            transactionExecutor = new PessimisticTransactionExecutor(1);
        }
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) {
        transactionExecutor.executeOperation(key, transaction,
                (taskResult) -> {
                    record.metadata.vote = record.metadata.vote.merge(taskResult.vote);
                    collector.safeCollect(record);
                });
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        transactionExecutor.onGlobalTermination(transaction);
    }
}
