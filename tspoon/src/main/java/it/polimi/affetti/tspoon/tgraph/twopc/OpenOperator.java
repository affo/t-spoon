package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.metrics.SingleValueAccumulator;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by affo on 14/07/17.
 */
public class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>>,
        OpenOperatorTransactionCloseListener {
    private static final String NUMBER_OF_CLOSED_TRANSACTIONS = "total-closed-transactions";
    private static final String DEPENDENCY_REPLAYED_COUNTER_NAME = "replayed-upon-dependency-satisfaction";
    private static final String DIRECTLY_REPLAYED_COUNTER_NAME = "directly-replayed";
    private static final String REPLAYED_PERCENTAGE = "replayed-percentage";
    private static final String REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME = "replayed-upon-watermark-update";

    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };
    public final OutputTag<Tuple2<Long, Vote>> logTag = new OutputTag<Tuple2<Long, Vote>>("tLog") {
    };

    private int lastCommittedWatermark = 0;
    // TODO temporarly avoiding log ordering
    //protected transient InOrderSideCollector<T, Tuple2<Long, Vote>> collector;
    protected transient SafeCollector<T> collector;

    // tid -> dependent tid (if mapping t1 -> t2 is present, this means that t2 depends on t1)
    private Map<Integer, Integer> dependencies = new HashMap<>();
    // timestamp -> current watermark
    private Map<Integer, Integer> playedWithWatermark = new HashMap<>();
    private Set<Integer> laterReplay = new HashSet<>();

    protected final TRuntimeContext tRuntimeContext;
    protected final TransactionsIndex<T> transactionsIndex;
    private transient AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;

    // stats
    private IntCounter numberOfClosedTransactionsClosed = new IntCounter();
    private IntCounter replayedUponWatermarkUpdate = new IntCounter();
    private IntCounter replayedUponDependencySatisfaction = new IntCounter();
    private IntCounter directlyReplayed = new IntCounter();
    private SingleValueAccumulator<Double> replayedPercentage = new SingleValueAccumulator<>(0.0);
    private Map<Vote, IntCounter> stats = new HashMap<>();

    public OpenOperator(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
        this.transactionsIndex = tRuntimeContext.getTransactionsIndex();

        for (Vote vote : Vote.values()) {
            stats.put(vote, new IntCounter());
            Report.registerAccumulator(vote.toString().toLowerCase() + "-counter");
        }

        Report.registerAccumulator(NUMBER_OF_CLOSED_TRANSACTIONS);
        Report.registerAccumulator(DEPENDENCY_REPLAYED_COUNTER_NAME);
        Report.registerAccumulator(REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME);
        Report.registerAccumulator(DIRECTLY_REPLAYED_COUNTER_NAME);
        Report.registerAccumulator(REPLAYED_PERCENTAGE);
    }

    @Override
    public void open() throws Exception {
        super.open();
        // TODO temporarly avoiding log ordering
        // collector = new InOrderSideCollector<>(output, logTag);
        collector = new SafeCollector<>(output);
        openOperatorTransactionCloser = tRuntimeContext.getSourceTransactionCloser();
        openOperatorTransactionCloser.open();

        if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            openOperatorTransactionCloser.subscribe(this);
        }

        // register accumulators
        for (Map.Entry<Vote, IntCounter> s : stats.entrySet()) {
            Vote vote = s.getKey();
            getRuntimeContext().addAccumulator(vote.toString().toLowerCase() + "-counter", s.getValue());
        }

        getRuntimeContext().addAccumulator(NUMBER_OF_CLOSED_TRANSACTIONS, numberOfClosedTransactionsClosed);
        getRuntimeContext().addAccumulator(DEPENDENCY_REPLAYED_COUNTER_NAME, replayedUponDependencySatisfaction);
        getRuntimeContext().addAccumulator(REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME, replayedUponWatermarkUpdate);
        getRuntimeContext().addAccumulator(DIRECTLY_REPLAYED_COUNTER_NAME, directlyReplayed);
        getRuntimeContext().addAccumulator(REPLAYED_PERCENTAGE, replayedPercentage);
    }

    @Override
    public void close() throws Exception {
        super.close();
        openOperatorTransactionCloser.close();
    }

    @Override
    public synchronized void processElement(StreamRecord<T> sr) throws Exception {
        T element = sr.getValue();
        TransactionsIndex<T>.LocalTransactionContext tContext = transactionsIndex.newTransaction(element);
        collect(tContext);
    }

    private void collect(TransactionsIndex<T>.LocalTransactionContext transactionContext) {
        int tid = transactionContext.tid;
        Metadata metadata = new Metadata(tid);
        metadata.timestamp = transactionContext.timestamp;
        metadata.coordinator = openOperatorTransactionCloser.getServerAddress();
        metadata.watermark = transactionsIndex.getCurrentWatermark();

        if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.SPECIFIC) {
            openOperatorTransactionCloser.subscribeTo(metadata.timestamp, this);
        }

        playedWithWatermark.put(metadata.tid, metadata.watermark);
        collector.safeCollect(Enriched.of(metadata, transactionContext.element));
    }

    private void collect(int tid) {
        T element = transactionsIndex.getTransaction(tid).element;
        TransactionsIndex<T>.LocalTransactionContext tContext = transactionsIndex.newTransaction(element, tid);
        this.collect(tContext);
    }

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

    private void updateStats(Vote vote) {
        stats.get(vote).add(1);

        if (vote != Vote.REPLAY) {
            numberOfClosedTransactionsClosed.add(1);
        }

        double totalTransactions = numberOfClosedTransactionsClosed.getLocalValue();
        double replayed = stats.get(Vote.REPLAY).getLocalValue();
        replayedPercentage.update((replayed / totalTransactions) * 100.0);
    }

    @Override
    public void onCloseTransaction(CloseTransactionNotification notification) {
        TransactionsIndex<T>.LocalTransactionContext localTransactionContext = transactionsIndex
                .getTransactionByTimestamp(notification.timestamp);

        updateStats(notification.vote);

        int tid = localTransactionContext.tid;
        int timestamp = localTransactionContext.timestamp;
        Vote vote = notification.vote;
        Integer dependency = transactionsIndex.getTransactionId(notification.replayCause);
        boolean dependencyNotSatisfied = dependency != null; // the transaction has not yet committed/aborted

        int oldWM = transactionsIndex.getCurrentWatermark();
        int newWM = transactionsIndex.updateWatermark(timestamp, vote);
        boolean wmUpdate = newWM > oldWM;

        if (wmUpdate) {
            laterReplay.forEach(id -> {
                collect(id);
                replayedUponWatermarkUpdate.add(1); // stats
            });
            laterReplay.clear();
        }

        switch (vote) {
            case COMMIT:
                if (timestamp > lastCommittedWatermark) {
                    lastCommittedWatermark = timestamp;
                }

                if (newWM >= lastCommittedWatermark) {
                    collector.safeCollect(watermarkTag, lastCommittedWatermark);
                }
            case ABORT:
                // when committing/aborting you can delete the transaction
                transactionsIndex.deleteTransaction(tid);
                satisfyDependency(tid);
                break;
            case REPLAY:
                // this transaction depends on a previous one
                if (dependencyNotSatisfied && tid > dependency) {
                    addDependency(tid, dependency);
                } else {
                    directlyReplayed.add(1);
                    replayElement(tid);
                }
        }

        /* TODO temporarly avoiding log ordering
        collector.collectInOrder(Tuple2.of(ts, notification.vote), ts);
        collector.flushOrdered(transactionsIndex.getCurrentWatermark());
        */
        collector.safeCollect(logTag,
                Tuple2.of((long) notification.timestamp, notification.vote));
    }

    private void replayElement(Integer tid) {
        if (tRuntimeContext.getStrategy() == Strategy.OPTIMISTIC) {
            // do not replay with the same watermark...
            // let something change before replay!
            int playedWithWatermark = this.playedWithWatermark.remove(tid);
            if (playedWithWatermark < transactionsIndex.getCurrentWatermark()) {
                collect(tid);
            } else {
                laterReplay.add(tid);
            }
        } else {
            // in the pessimistic case there is no need to worry about the watermark
            collect(tid);
        }
    }

    // The first depends on the second one (dependsOn > tid)
    private void addDependency(int dependsOn, int tid) {
        Integer alreadyDependent = dependencies.get(tid);

        if (alreadyDependent == null) {
            dependencies.put(tid, dependsOn);
            return;
        }

        if (dependsOn <= alreadyDependent) {
            // insert a step in the dependency chain
            dependencies.put(tid, dependsOn);
            dependencies.put(dependsOn, alreadyDependent);
            return;
        }

        // dependsOn can depend on the transaction that already depends on tid:
        // extend the dependency chain
        addDependency(dependsOn, alreadyDependent);
    }

    private void satisfyDependency(int tid) {
        Integer dependent = dependencies.remove(tid);

        if (dependent != null) {
            replayElement(dependent);
            replayedUponDependencySatisfaction.add(1);
        }
    }
}
