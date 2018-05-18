package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.metrics.*;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.JobControlListener;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.durability.SnapshotService;
import it.polimi.affetti.tspoon.tgraph.durability.WALEntry;
import it.polimi.affetti.tspoon.tgraph.durability.WALService;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by affo on 14/07/17.
 */
public class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>>,
        OpenOperatorTransactionCloseListener, JobControlListener {
    private transient Logger LOG;

    private static final String COMMIT_COUNT = "commit-count";
    private static final String ABORT_COUNT = "abort-count";
    private static final String REPLAY_COUNT = "replay-count";
    private static final String NUMBER_OF_CLOSED_TRANSACTIONS = "total-closed-transactions";
    private static final String DEPENDENCY_REPLAYED_COUNTER_NAME = "replayed-upon-dependency-satisfaction";
    private static final String DIRECTLY_REPLAYED_COUNTER_NAME = "directly-replayed";
    private static final String REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME = "replayed-upon-watermark-update";
    private static final String CLOSED_REPLAYED_RATIO = "closed-replayed-ratio";
    private static final String REPLAYED_TWICE = "replayed-2";
    private static final String REPLAYED_TRICE = "replayed-3";
    private static final String REPLAYED_3_PLUS = "replayed-3+";
    private static final String PROTOCOL_LATENCY = "open2open-latency";

    public final OutputTag<Long> watermarkTag = new OutputTag<Long>("watermark") {
    };

    private int sourceID;
    private final int tGraphID;
    private long lastCommittedWatermark = Long.MIN_VALUE;
    private long restoredTid = 0;
    private Set<Long> intraEpochTids = new HashSet<>(); // to discard WALled tnxs
    protected transient SafeCollector<T> collector;

    private DependencyTracker dependencyTracker = new DependencyTracker();
    // tid -> current watermark
    private Map<Long, Long> playedWithWatermark = new HashMap<>();
    private Set<Long> laterReplay = new HashSet<>();
    private transient TimestampGenerator timestampGenerator;

    protected final TRuntimeContext tRuntimeContext;
    protected TransactionsIndex<T> transactionsIndex;
    private transient AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;

    private transient JobControlClient jobControlClient;
    private transient SnapshotService snapshotService;

    // stats
    private IntCounter commits = new IntCounter();
    private IntCounter aborts = new IntCounter();
    private IntCounter replays = new IntCounter();
    private IntCounter numberOfClosedTransactions = new IntCounter();
    private IntCounter replayedUponWatermarkUpdate = new IntCounter();
    private IntCounter replayedUponDependencySatisfaction = new IntCounter();
    private IntCounter directlyReplayed = new IntCounter();
    private SingleValueAccumulator<Double> replayedRatio = new SingleValueAccumulator<>(0.0);

    private Map<Long, Integer> replayCounts = new HashMap<>();
    private IntCounter replayedTwice = new IntCounter();
    private IntCounter replayedTrice = new IntCounter();
    private IntCounter replayedTooMuch = new IntCounter();

    private TimeDelta currentLatency = new TimeDelta();

    private RealTimeAccumulatorsWithPerBatchCurve accumulators = new RealTimeAccumulatorsWithPerBatchCurve();

    private MetricAccumulator numberOfWalEntriesReplayed = new MetricAccumulator();
    private MetricAccumulator recoveryTime = new MetricAccumulator();

    public OpenOperator(TRuntimeContext tRuntimeContext, int tGraphID) {
        this.tGraphID = tGraphID;
        this.tRuntimeContext = tRuntimeContext;

        Report.registerAccumulator(COMMIT_COUNT);
        Report.registerAccumulator(ABORT_COUNT);
        Report.registerAccumulator(REPLAY_COUNT);
        Report.registerAccumulator(NUMBER_OF_CLOSED_TRANSACTIONS);
        Report.registerAccumulator(DEPENDENCY_REPLAYED_COUNTER_NAME);
        Report.registerAccumulator(REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME);
        Report.registerAccumulator(DIRECTLY_REPLAYED_COUNTER_NAME);
        Report.registerAccumulator(CLOSED_REPLAYED_RATIO);
        Report.registerAccumulator(PROTOCOL_LATENCY);
    }

    @Override
    public void open() throws Exception {
        super.open();
        // NOTE: this happens __after__ initializeState
        this.sourceID = getRuntimeContext().getIndexOfThisSubtask();
        int numberOfSources = getRuntimeContext().getNumberOfParallelSubtasks();

        collector = new SafeCollector<>(output);
        openOperatorTransactionCloser = tRuntimeContext.getSourceTransactionCloser(this.sourceID);
        openOperatorTransactionCloser.open();

        if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            openOperatorTransactionCloser.subscribe(this);
        }

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        try {
            jobControlClient = JobControlClient.get(parameterTool);
            jobControlClient.observe(this);
        } catch (IllegalArgumentException iae) {
            LOG.warn("Starting without listening to JobControl events" +
                    "because cannot connect to JobControl: " + iae.getMessage());
            jobControlClient = null;
        }

        WALService walService = tRuntimeContext.getWALService();
        snapshotService = tRuntimeContext.getSnapshotService(parameterTool);

        if (tRuntimeContext.isDurabilityEnabled()) {
            getRuntimeContext().addAccumulator("recovery-time", recoveryTime);
            getRuntimeContext().addAccumulator("number-of-wal-entries-replayed", numberOfWalEntriesReplayed);
        }

        long start = System.nanoTime();
        // restore completedTids in the lastSnapshot
        int numberOfWalEntries = 0;
        long maxTimestamp = -1;
        Iterator<WALEntry> replay = walService.replay(sourceID, numberOfSources); // the entries for my source ID
        while (replay.hasNext()) {
            WALEntry walEntry = replay.next();
            intraEpochTids.add(walEntry.tid);
            numberOfWalEntries++;
            maxTimestamp = Math.max(maxTimestamp, walEntry.timestamp);
        }
        double delta = (System.nanoTime() - start) / Math.pow(10, 6); // ms
        recoveryTime.add(delta);
        numberOfWalEntriesReplayed.add((double) numberOfWalEntries);
        walService.close();

        if (maxTimestamp < 0) {
            maxTimestamp = sourceID;
        }

        timestampGenerator = new TimestampGenerator(sourceID, numberOfSources, maxTimestamp);
        transactionsIndex = tRuntimeContext.getTransactionsIndex(restoredTid, timestampGenerator);

        accumulators.register(getRuntimeContext(), COMMIT_COUNT, commits);
        accumulators.register(getRuntimeContext(), ABORT_COUNT, aborts);
        accumulators.register(getRuntimeContext(), REPLAY_COUNT, replays);
        accumulators.register(getRuntimeContext(), NUMBER_OF_CLOSED_TRANSACTIONS, numberOfClosedTransactions);
        accumulators.register(getRuntimeContext(), DEPENDENCY_REPLAYED_COUNTER_NAME, replayedUponDependencySatisfaction);
        accumulators.register(getRuntimeContext(), REPLAYED_UPON_WATERMARK_UPDATE_COUNTER_NAME, replayedUponWatermarkUpdate);
        accumulators.register(getRuntimeContext(), DIRECTLY_REPLAYED_COUNTER_NAME, directlyReplayed);
        accumulators.register(getRuntimeContext(), CLOSED_REPLAYED_RATIO, replayedRatio);
        accumulators.register(getRuntimeContext(), PROTOCOL_LATENCY, new MetricAccumulator(currentLatency.getMetric()));
        accumulators.register(getRuntimeContext(), REPLAYED_TWICE, replayedTwice);
        accumulators.register(getRuntimeContext(), REPLAYED_TRICE, replayedTrice);
        accumulators.register(getRuntimeContext(), REPLAYED_3_PLUS, replayedTooMuch);
        accumulators.registerCurve(getRuntimeContext(), getRuntimeContext().getTaskName() + "-curve");
    }

    @Override
    public void close() throws Exception {
        super.close();
        openOperatorTransactionCloser.close();
        if (jobControlClient != null) {
            jobControlClient.close();
        }
    }

    @Override
    public synchronized void processElement(StreamRecord<T> sr) throws Exception {
        T element = sr.getValue();
        TransactionsIndex<T>.LocalTransactionContext tContext = transactionsIndex.newTransaction(element);

        // if in recovery mode we discard the transaction and update directly
        // if it was logged on the WALService
        if (intraEpochTids.remove(tContext.tid)) {
            transactionsIndex.updateWatermark(tContext.timestamp, Vote.COMMIT);
            transactionsIndex.deleteTransaction(tContext.tid);
        } else {
            collect(tContext);
        }
    }

    private void collect(TransactionsIndex<T>.LocalTransactionContext transactionContext) {
        long tid = transactionContext.tid;
        Metadata metadata = new Metadata(tGraphID, tid);
        metadata.originalRecord = transactionContext.element;
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

    private void collect(long tid) {
        T element = transactionsIndex.getTransaction(tid).element;
        TransactionsIndex<T>.LocalTransactionContext tContext = transactionsIndex.newTransaction(element, tid);
        this.collect(tContext);
    }

    // ----------------------------- Transaction close notification logic

    @Override
    public int getTGraphID() {
        return tGraphID;
    }

    private void updateStats(long timestamp, Vote vote) {
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
        if (replayed > 0) {
            replayedRatio.update(totalTransactions / replayed); // hope it's bigger than 1!
        }
    }

    @Override
    public synchronized void onCloseTransaction(CloseTransactionNotification notification) {
        TransactionsIndex<T>.LocalTransactionContext localTransactionContext = transactionsIndex
                .getTransactionByTimestamp(notification.timestamp);

        if (localTransactionContext == null) {
            if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.SPECIFIC) {
                // this means that this notification is not for me!
                throw new IllegalStateException("I subscribed to a SPECIFIC transaction" +
                        "that I didn't register in transactionIndex: " + notification.timestamp);
            }

            // this means that this notification is not for me!
            return;
        }

        if (!timestampGenerator.checkTimestamp(sourceID, notification.timestamp)) {
            // not for me
            return;
        }

        updateStats(notification.timestamp, notification.vote);

        long tid = localTransactionContext.tid;
        long timestamp = localTransactionContext.timestamp;
        Vote vote = notification.vote;
        long replayCause = notification.replayCause;
        boolean dependencyNotSatisfied = transactionsIndex.isTransactionRunning(replayCause);

        long oldWM = transactionsIndex.getCurrentWatermark();
        long newWM = transactionsIndex.updateWatermark(timestamp, vote);
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
                Long unleashed = dependencyTracker.satisfyDependency(tid);
                if (unleashed != null) {
                    replayElement(unleashed);
                    replayedUponDependencySatisfaction.add(1);
                }

                // when committing/aborting you can delete the transaction
                transactionsIndex.deleteTransaction(tid);
                playedWithWatermark.remove(tid);
                replayCounts.remove(tid);
                break;
            case REPLAY:
                // stats
                int replayCount = replayCounts.getOrDefault(tid, 0);
                replayCounts.put(tid, ++replayCount);
                if (replayCount == 2) {
                    replayedTwice.add(1);
                } else if (replayCount == 3) {
                    replayedTrice.add(1);
                } else if (replayCount > 3) {
                    replayedTooMuch.add(1);
                }

                // this transaction depends on a previous one (break cycles)
                if (dependencyNotSatisfied && replayCause < tid) {
                    dependencyTracker.addDependency(replayCause, tid);
                } else {
                    directlyReplayed.add(1);
                    replayElement(tid);
                }
        }
    }

    private void replayElement(Long tid) {
        if (tRuntimeContext.isBaselineMode()) {
            // does nothing
            return;
        }

        if (tRuntimeContext.getStrategy() == Strategy.OPTIMISTIC &&
                tRuntimeContext.getIsolationLevel() != IsolationLevel.PL4) {
            // do not replay with the same watermark...
            // let something change before replay!
            long playedWithWatermark = this.playedWithWatermark.remove(tid);
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
        accumulators.addPoint();
        currentLatency.reset();
    }

    // --------------------------------------- Recovery & Snapshotting

    private ListState<Long> startTid;

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        long currentWatermark;
        synchronized (this) {
            // save start Tid for this snapshot
            startTid.clear();
            startTid.add(transactionsIndex.getCurrentTid());

            // save watermark for snapshotting at state operators
            currentWatermark = transactionsIndex.getCurrentWatermark();
        }

        LOG.info("Starting snapshot at " + sourceID + " [wm: " + currentWatermark + "]");
        snapshotService.startSnapshot(currentWatermark);
        LOG.info("Snapshot started [wm: " + currentWatermark + "]");
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        LOG = Logger.getLogger(Thread.currentThread().getName());
        startTid = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("tid", Long.class));

        for (Long tid : startTid.get()) {
            restoredTid = tid;
        }

        LOG.info("Restored tid: " + restoredTid);
    }
}
