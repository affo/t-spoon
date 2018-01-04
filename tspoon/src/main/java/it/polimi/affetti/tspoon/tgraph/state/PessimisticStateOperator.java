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
    private transient PessimisticTransactionExecutor<Enriched<T>> transactionExecutor;
    private transient Thread emitterThread;
    private transient Emitter emitter;
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
            transactionExecutor = new PessimisticTransactionExecutor<>(1, deadlockTimeout);
        } else {
            transactionExecutor = new PessimisticTransactionExecutor<>(1);
        }

        emitter = new Emitter();
        emitterThread = new Thread(emitter);
        emitterThread.setName("RecordEmitter for " + Thread.currentThread().getName());
        emitterThread.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        emitter.cancel();
        emitterThread.interrupt();
        emitterThread.join();
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) {
        transactionExecutor.executeOperation(key, transaction, record);
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        transactionExecutor.onGlobalTermination(transaction);
    }

    /**
     * TODO maybe find a way to fix it in the future.
     * The only way to prevent serialization/deserialization problems given by emitting records from
     * multiple threads turned out to be:
     *  do not use the safeCollector, but use, instead, a single thread that emits every record
     *  produced by the transaction executor.
     */
    private class Emitter implements Runnable {
        private volatile boolean stop = false;

        @Override
        public void run() {
            try {
                while (!stop) {
                    PessimisticTransactionExecutor<Enriched<T>>
                            .OperationExecutionResult result = transactionExecutor.getResult();
                    Enriched<T> out = result.futureResult;
                    out.metadata.vote = out.metadata.vote.merge(result.vote);
                    out.metadata.dependencyTracking = result.getReplayCauses();
                    collector.safeCollect(out);
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted while emitting: " + e.getMessage());
            }
        }

        public void cancel() {
            stop = true;
        }
    }
}
