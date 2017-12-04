package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;

/**
 * Created by affo on 16/11/17.
 */
public class PessimisticOpenOperator<T> extends OpenOperator<T> {
    public PessimisticOpenOperator(AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser) {
        // fix the transaction index (timestamping is useless for pessimistic)
        super(new TidTransactionsIndex<>(), openOperatorTransactionCloser);
    }

    @Override
    protected void onOpenTransaction(T recordValue, Metadata metadata) {
        // nothing more
    }

    @Override
    protected void closeTransaction(TransactionsIndex.LocalTransactionContext transactionContext) {
        int tid = transactionContext.tid;
        int timestamp = transactionContext.timestamp;
        Vote vote = transactionContext.vote;

        if (vote == Vote.REPLAY) {
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
