package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.InOrderSideCollector;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

import static it.polimi.affetti.tspoon.tgraph.twopc.TransactionsIndex.LocalTransactionContext;

/**
 * Created by affo on 14/07/17.
 */
public abstract class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>>,
        CloseTransactionListener {
    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };
    public final OutputTag<Tuple2<Long, Vote>> logTag = new OutputTag<Tuple2<Long, Vote>>("wal") {
    };

    protected transient InOrderSideCollector<T, Tuple2<Long, Vote>> collector;

    protected final TransactionsIndex transactionsIndex;
    private final CoordinatorTransactionCloser coordinatorTransactionCloser;

    // stats
    protected Map<Vote, IntCounter> stats = new HashMap<>();

    public OpenOperator(
            TransactionsIndex transactionsIndex,
            CoordinatorTransactionCloser coordinatorTransactionCloser) {
        this.coordinatorTransactionCloser = coordinatorTransactionCloser;
        this.transactionsIndex = transactionsIndex;

        for (Vote vote : Vote.values()) {
            stats.put(vote, new IntCounter());
            Report.registerAccumulator(vote.toString().toLowerCase() + "-counter");
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new InOrderSideCollector<>(output, logTag);
        coordinatorTransactionCloser.open(this);

        // register accumulators
        for (Map.Entry<Vote, IntCounter> s : stats.entrySet()) {
            Vote vote = s.getKey();
            getRuntimeContext().addAccumulator(vote.toString().toLowerCase() + "-counter", s.getValue());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        coordinatorTransactionCloser.close();
    }

    @Override
    public synchronized void processElement(StreamRecord<T> sr) throws Exception {
        LocalTransactionContext tContext = transactionsIndex.newTransaction();
        Metadata metadata = getMetadataFromContext(tContext);
        onOpenTransaction(sr.getValue(), metadata);
        collector.safeCollect(sr.replace(Enriched.of(metadata, sr.getValue())));
    }

    protected Metadata getMetadataFromContext(LocalTransactionContext tContext) {
        Metadata metadata = new Metadata(tContext.tid);
        metadata.timestamp = tContext.timestamp;
        metadata.coordinator = coordinatorTransactionCloser.getAddress();
        metadata.watermark = transactionsIndex.getCurrentWatermark();
        return metadata;
    }

    protected abstract void onOpenTransaction(T recordValue, Metadata metadata);

    @Override
    public synchronized void onCloseTransaction(CloseTransactionNotification notification) {
        LocalTransactionContext localTransactionContext = transactionsIndex
                .getTransactionByTimestamp(notification.timestamp);
        localTransactionContext.replayCause = notification.replayCause;
        localTransactionContext.vote = notification.vote;

        closeTransaction(localTransactionContext);

        long ts = (long) notification.timestamp;
        collector.collectInOrder(Tuple2.of(ts, notification.vote), ts);
        collector.flushOrdered(transactionsIndex.getCurrentWatermark());
    }

    protected abstract void closeTransaction(LocalTransactionContext transactionContext);
}
