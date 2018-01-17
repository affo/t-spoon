package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.metrics.*;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.JobControlListener;
import it.polimi.affetti.tspoon.tgraph.*;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.sling.commons.json.JSONObject;

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
        OpenOperatorTransactionCloseListener, JobControlListener {
    private static final String COMMIT_COUNT = "commit-count";
    private static final String ABORT_COUNT = "abort-count";
    private static final String REPLAY_COUNT = "replay-count";
    private static final String NUMBER_OF_CLOSED_TRANSACTIONS = "total-closed-transactions";
    private static final String DEPENDENCY_REPLAYED_COUNTER_NAME = "replayed-upon-dependency-satisfaction";
    private static final String DIRECTLY_REPLAYED_COUNTER_NAME = "directly-replayed";
    private static final String REPLAYED_PERCENTAGE = "replayed-percentage";
    private static final String REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME = "replayed-upon-watermark-update";
    private static final String PROTOCOL_LATENCY = "open2open-latency";
    private static final String WATERMARK_REFRESH_RATE = "watermark-refresh-rate";

    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };
    public final OutputTag<Tuple2<Long, Vote>> logTag = new OutputTag<Tuple2<Long, Vote>>("tLog") {
    };

    private int lastCommittedWatermark = 0;
    // TODO temporarly avoiding log ordering
    //protected transient InOrderSideCollector<T, Tuple2<Long, Vote>> collector;
    protected transient SafeCollector<T> collector;

    private DependencyTracker dependencyTracker = new DependencyTracker();
    // tid -> current watermark
    private Map<Integer, Integer> playedWithWatermark = new HashMap<>();
    private Set<Integer> laterReplay = new HashSet<>();

    protected final TRuntimeContext tRuntimeContext;
    protected final TransactionsIndex<T> transactionsIndex;
    private transient AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;

    private transient JobControlClient jobControlClient;
    private int batchNumber = 0;

    // stats
    private IntCounter commits = new IntCounter();
    private IntCounter aborts = new IntCounter();
    private IntCounter replays = new IntCounter();
    private IntCounter numberOfClosedTransactions = new IntCounter();
    private IntCounter replayedUponWatermarkUpdate = new IntCounter();
    private IntCounter replayedUponDependencySatisfaction = new IntCounter();
    private IntCounter directlyReplayed = new IntCounter();
    private SingleValueAccumulator<Double> replayedPercentage = new SingleValueAccumulator<>(0.0);
    private TimeDelta currentLatency = new TimeDelta();

    private Map<String, Accumulator<?, ?>> accumulators = new HashMap<>();
    private Map<String, MetricCurveAccumulator> curves = new HashMap<>();

    public OpenOperator(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
        this.transactionsIndex = tRuntimeContext.getTransactionsIndex();

        Report.registerAccumulator(COMMIT_COUNT);
        Report.registerAccumulator(ABORT_COUNT);
        Report.registerAccumulator(REPLAY_COUNT);
        Report.registerAccumulator(NUMBER_OF_CLOSED_TRANSACTIONS);
        Report.registerAccumulator(DEPENDENCY_REPLAYED_COUNTER_NAME);
        Report.registerAccumulator(REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME);
        Report.registerAccumulator(DIRECTLY_REPLAYED_COUNTER_NAME);
        Report.registerAccumulator(REPLAYED_PERCENTAGE);
        Report.registerAccumulator(PROTOCOL_LATENCY);
        Report.registerAccumulator(WATERMARK_REFRESH_RATE);
    }

    private void registerAccumulator(String name, Accumulator<?, ?> accumulator) {
        MetricCurveAccumulator curve = new MetricCurveAccumulator();
        accumulators.put(name, accumulator);
        curves.put(name, curve);

        String curveName = name + "-curve";
        getRuntimeContext().addAccumulator(name, accumulator);
        getRuntimeContext().addAccumulator(curveName, curve);
    }

    @Override
    public void open() throws Exception {
        super.open();
        // TODO temporarly avoiding log ordering
        // collector = new InOrderSideCollector<>(output, logTag);
        collector = new SafeCollector<>(output);
        // everybody shares the same OpenServer by specifying the same taskNumber
        openOperatorTransactionCloser = tRuntimeContext.getSourceTransactionCloser(0);
        openOperatorTransactionCloser.open();

        if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            openOperatorTransactionCloser.subscribe(this);
        }

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.observe(this);

        // register accumulators
        registerAccumulator(COMMIT_COUNT, commits);
        registerAccumulator(ABORT_COUNT, aborts);
        registerAccumulator(REPLAY_COUNT, replays);
        registerAccumulator(NUMBER_OF_CLOSED_TRANSACTIONS, numberOfClosedTransactions);
        registerAccumulator(DEPENDENCY_REPLAYED_COUNTER_NAME, replayedUponDependencySatisfaction);
        registerAccumulator(REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME, replayedUponWatermarkUpdate);
        registerAccumulator(DIRECTLY_REPLAYED_COUNTER_NAME, directlyReplayed);
        registerAccumulator(REPLAYED_PERCENTAGE, replayedPercentage);
        registerAccumulator(PROTOCOL_LATENCY, new MetricAccumulator(currentLatency.getMetric()));
    }

    @Override
    public void close() throws Exception {
        super.close();
        openOperatorTransactionCloser.close();
        jobControlClient.close();
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

        currentLatency.start(String.valueOf(metadata.timestamp));
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

    private void updateStats(int timestamp, Vote vote) {
        switch (vote) {
            case COMMIT:
                commits.add(1);
                break;
            case ABORT:
                aborts.add(1);
                break;
            case REPLAY:
                replays.add(1);
                break;
        }
        currentLatency.end(String.valueOf(timestamp));

        if (vote != Vote.REPLAY) {
            numberOfClosedTransactions.add(1);
        }

        double totalTransactions = numberOfClosedTransactions.getLocalValue();
        double replayed = replays.getLocalValue();
        replayedPercentage.update((replayed / totalTransactions) * 100.0);
    }

    @Override
    public void onCloseTransaction(CloseTransactionNotification notification) {
        TransactionsIndex<T>.LocalTransactionContext localTransactionContext = transactionsIndex
                .getTransactionByTimestamp(notification.timestamp);

        updateStats(notification.timestamp, notification.vote);

        int tid = localTransactionContext.tid;
        int timestamp = localTransactionContext.timestamp;
        Vote vote = notification.vote;
        int replayCause = notification.replayCause;
        boolean dependencyNotSatisfied = transactionsIndex.isTransactionRunning(replayCause);

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
                // committed/aborted transaction satisfies a dependency
                Integer unleashed = dependencyTracker.satisfyDependency(tid);
                if (unleashed != null) {
                    replayElement(unleashed);
                    replayedUponDependencySatisfaction.add(1);
                }

                // when committing/aborting you can delete the transaction
                transactionsIndex.deleteTransaction(tid);
                playedWithWatermark.remove(tid);
                break;
            case REPLAY:
                // this transaction depends on a previous one (break cycles)
                if (dependencyNotSatisfied && replayCause < tid) {
                    dependencyTracker.addDependency(replayCause, tid);
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
        if (tRuntimeContext.getStrategy() == Strategy.OPTIMISTIC &&
                tRuntimeContext.getIsolationLevel() != IsolationLevel.PL4) {
            // do not replay with the same watermark...
            // let something change before replay!
            int playedWithWatermark = this.playedWithWatermark.remove(tid);
            if (playedWithWatermark < transactionsIndex.getCurrentWatermark()) {
                collect(tid);
            } else {
                laterReplay.add(tid);
            }
        } else {
            // - In the pessimistic case there is no need to worry about the watermark
            // - In the case of PL4 optimistic, the watermark is the last completed tid.
            //   It could be the case that you need to REPLAY the same transaction twice under the same watermark:
            //   Think of forward dependencies for example, their REPLAY is not caused by a small watermark,
            //   but by being happened too early in time...
            collect(tid);
        }
    }

    @Override
    public synchronized void onBatchEnd() {
        addPointToCurveAndReset(COMMIT_COUNT);
        addPointToCurveAndReset(ABORT_COUNT);
        addPointToCurveAndReset(REPLAY_COUNT);
        addPointToCurveAndReset(NUMBER_OF_CLOSED_TRANSACTIONS);
        addPointToCurveAndReset(DEPENDENCY_REPLAYED_COUNTER_NAME);
        addPointToCurveAndReset(DIRECTLY_REPLAYED_COUNTER_NAME);
        addPointToCurveAndReset(REPLAYED_PERCENTAGE);
        addPointToCurveAndReset(REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME);
        addPointToCurveAndReset(PROTOCOL_LATENCY);
        currentLatency.reset();
        addPointToCurveAndReset(WATERMARK_REFRESH_RATE);

        batchNumber++;
    }

    private void addPointToCurveAndReset(String accumulatorName) {
        Accumulator<?, ?> accumulator = accumulators.get(accumulatorName);
        Object value;
        if (accumulator instanceof MetricAccumulator) {
            value = ((MetricAccumulator) accumulator).getLocalValue().metric.getMean();
        } else {
            value = accumulator.getLocalValue();
        }
        addPointToCurveAndReset(accumulatorName, value);
    }

    private void addPointToCurveAndReset(String accumulatorName, Object value) {
        Accumulator<?, ?> accumulator = accumulators.get(accumulatorName);
        MetricCurveAccumulator curve = curves.get(accumulatorName);
        curve.add(Point.of(batchNumber, value));
        accumulator.resetLocal();
    }

    private static class Point extends MetricCurveAccumulator.Point {
        public final int batchNumber;
        public final Object value;

        public Point(int batchNumber, Object value) {
            this.batchNumber = batchNumber;
            this.value = value;
        }

        public static Point of(int batchNumber, Object value) {
            return new Point(batchNumber, value);
        }

        @Override
        public JSONObject toJSON() {
            Map<String, Object> map = new HashMap<>();
            map.put("batchNumber", batchNumber);
            map.put("value", value);
            return new JSONObject(map);
        }
    }
}
