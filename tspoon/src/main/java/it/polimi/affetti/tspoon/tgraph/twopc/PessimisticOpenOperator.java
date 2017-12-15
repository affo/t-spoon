package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.accumulators.IntCounter;

/**
 * Created by affo on 16/11/17.
 */
public class PessimisticOpenOperator<T> extends OpenOperator<T> {
    public static final String REPLAYED_TRANSACTIONS = "replayed_for_deadlock";
    private IntCounter replayed;

    public PessimisticOpenOperator(TwoPCRuntimeContext twoPCRuntimeContext) {
        // fix the transaction index (timestamping is useless for pessimistic)
        super(new TidTransactionsIndex<>(), twoPCRuntimeContext);

        replayed = new IntCounter();
        Report.registerAccumulator(REPLAYED_TRANSACTIONS);
    }

    @Override
    public void open() throws Exception {
        super.open();
        getRuntimeContext().addAccumulator(REPLAYED_TRANSACTIONS, replayed);
    }

    @Override
    protected void onOpenTransaction(T recordValue, Metadata metadata) {
        // nothing more
    }

    @Override
    protected void closeTransaction(TransactionsIndex.LocalTransactionContext transactionContext) {
        int tid = transactionContext.tid;
        Vote vote = transactionContext.vote;

        if (vote == Vote.REPLAY) {
            replayed.add(1);

            T element = transactionsIndex.getTransaction(tid).element;
            Metadata metadata = new Metadata(tid);
            metadata.timestamp = tid;
            metadata.coordinator = getCoordinatorAddress();
            collector.safeCollect(Enriched.of(metadata, element));
            return;
        }

        transactionsIndex.deleteTransaction(tid);
    }
}
