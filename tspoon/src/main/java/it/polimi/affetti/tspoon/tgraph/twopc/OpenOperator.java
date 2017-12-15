package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.twopc.TransactionsIndex.LocalTransactionContext;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 14/07/17.
 */
public abstract class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>>,
        OpenOperatorTransactionCloseListener {
    public final OutputTag<Tuple2<Long, Vote>> logTag = new OutputTag<Tuple2<Long, Vote>>("tLog") {
    };

    // TODO temporarly avoiding log ordering
    //protected transient InOrderSideCollector<T, Tuple2<Long, Vote>> collector;
    protected transient SafeCollector<T> collector;

    protected final TwoPCRuntimeContext twoPCRuntimeContext;
    protected final TransactionsIndex<T> transactionsIndex;
    private transient AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;

    // stats
    protected Map<Vote, IntCounter> stats = new HashMap<>();

    public OpenOperator(
            TransactionsIndex<T> transactionsIndex,
            TwoPCRuntimeContext twoPCRuntimeContext) {
        this.twoPCRuntimeContext = twoPCRuntimeContext;
        this.transactionsIndex = transactionsIndex;

        for (Vote vote : Vote.values()) {
            stats.put(vote, new IntCounter());
            Report.registerAccumulator(vote.toString().toLowerCase() + "-counter");
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        // TODO temporarly avoiding log ordering
        // collector = new InOrderSideCollector<>(output, logTag);
        collector = new SafeCollector<>(output);
        openOperatorTransactionCloser = twoPCRuntimeContext.getSourceTransactionCloser();
        openOperatorTransactionCloser.open();

        if (twoPCRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            openOperatorTransactionCloser.subscribe(this);
        }

        // register accumulators
        for (Map.Entry<Vote, IntCounter> s : stats.entrySet()) {
            Vote vote = s.getKey();
            getRuntimeContext().addAccumulator(vote.toString().toLowerCase() + "-counter", s.getValue());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        openOperatorTransactionCloser.close();
    }

    private void updateStats(Vote vote) {
        stats.get(vote).add(1);
    }

    protected void subscribe(long timestamp) {
        if (twoPCRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.SPECIFIC) {
            openOperatorTransactionCloser.subscribeTo(timestamp, this);
        }
    }

    @Override
    public synchronized void processElement(StreamRecord<T> sr) throws Exception {
        T element = sr.getValue();
        LocalTransactionContext tContext = transactionsIndex.newTransaction(element);
        Metadata metadata = new Metadata(tContext.tid);
        metadata.timestamp = tContext.timestamp;
        metadata.coordinator = getCoordinatorAddress();
        metadata.watermark = transactionsIndex.getCurrentWatermark();

        subscribe(tContext.timestamp);

        onOpenTransaction(element, metadata);
        collector.safeCollect(sr.replace(Enriched.of(metadata, element)));
    }

    protected Address getCoordinatorAddress() {
        return openOperatorTransactionCloser.getServerAddress();
    }

    protected abstract void onOpenTransaction(T recordValue, Metadata metadata);

    // ----------------------------- Transaction close notification logic

    @Override
    public Object getMonitorForUpdateLogic() {
        // synchronize with this when applying update logic
        return this;
    }

    // no need to synchronize because they are invoked atomically on notification
    @Override
    public boolean isInterestedIn(long timestamp) {
        return transactionsIndex
                .getTransactionByTimestamp((int) timestamp) != null;
    }

    @Override
    public void onCloseTransaction(CloseTransactionNotification notification) {
        LocalTransactionContext localTransactionContext = transactionsIndex
                .getTransactionByTimestamp(notification.timestamp);
        localTransactionContext.replayCause = notification.replayCause;
        localTransactionContext.vote = notification.vote;

        updateStats(notification.vote);

        closeTransaction(localTransactionContext);

        long ts = (long) notification.timestamp;
        /* TODO temporarly avoiding log ordering
        collector.collectInOrder(Tuple2.of(ts, notification.vote), ts);
        collector.flushOrdered(transactionsIndex.getCurrentWatermark());
        */
        collector.safeCollect(logTag, Tuple2.of(ts, notification.vote));
    }

    protected abstract void closeTransaction(LocalTransactionContext transactionContext);
}
