package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.db.PessimisticTransactionExecutor;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;
import it.polimi.affetti.tspoon.tgraph.twopc.TRuntimeContext;
import org.apache.flink.util.OutputTag;

import java.util.HashSet;

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

    public PessimisticStateOperator(
            int tGraphID,
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            TRuntimeContext tRuntimeContext) {
        super(tGraphID, nameSpace, stateFunction, updatesTag, tRuntimeContext);
    }

    @Override
    public void open() throws Exception {
        super.open();
        transactionExecutor = new PessimisticTransactionExecutor(1);
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) throws Exception {
        transactionExecutor.executeOperation(key, transaction,
                () -> {
                    record.metadata.vote = transaction.vote;
                    record.metadata.dependencyTracking = new HashSet<>(transaction.getDependencies());
                    decorateRecordWithUpdates(key, record, transaction);
                    collector.safeCollect(record);
                });
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        transactionExecutor.onGlobalTermination(transaction);
    }
}
