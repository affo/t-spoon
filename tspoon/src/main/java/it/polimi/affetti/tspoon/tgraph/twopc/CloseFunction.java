package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.durability.FileWAL;
import it.polimi.affetti.tspoon.tgraph.durability.LocalWALServer;
import it.polimi.affetti.tspoon.tgraph.durability.SnapshotService;
import it.polimi.affetti.tspoon.tgraph.durability.WALEntry;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by affo on 18/07/17.
 */
public class CloseFunction extends RichFlatMapFunction<Metadata, TransactionResult>
        implements CheckpointListener, CheckpointedFunction {
    private transient Logger LOG;
    private TRuntimeContext tRuntimeContext;
    private transient SnapshotService snapshotService;
    private transient LocalWALServer localWALServer;
    private transient FileWAL wal;
    private transient AbstractCloseOperatorTransactionCloser transactionCloser;
    private transient JobControlClient jobControlClient;
    private boolean restored = false, simulating;

    private List<TransactionResult> toReplay;

    // stats
    private IntCounter replays = new IntCounter();

    public CloseFunction(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
        this.simulating = tRuntimeContext.getRecoverySimulationRate() > 0;
        this.toReplay = new ArrayList<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.restored = functionInitializationContext.isRestored();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        if (tRuntimeContext.isDurabilityEnabled()) {
            localWALServer = tRuntimeContext.getLocalWALServer(parameterTool);
            wal = localWALServer.addAndCreateWAL(tRuntimeContext.getGraphId(), simulating || !restored);

            // fill the WALs with fake entries
            if (simulating) {
                long start = System.nanoTime();
                fillWALs(tRuntimeContext.getRecoverySimulationRate(), TransactionEnvironment.DEFAULT_CHECKPOINT_INTERVAL_MS,
                        tRuntimeContext.getNumberOfSources(), getRuntimeContext());
                double delta = (System.nanoTime() - start) * Math.pow(10, -6);

                MetricAccumulator createWALTime = new MetricAccumulator();
                createWALTime.add(delta);
                getRuntimeContext().addAccumulator("fake-wal-creation-and-reload-time-at-sink", createWALTime);
            }

            MetricAccumulator loadWALTime = new MetricAccumulator();
            loadWALTime.add((double) wal.getLoadWALTime());
            getRuntimeContext().addAccumulator("load-wal-time-at-sink", loadWALTime);

            long start = System.nanoTime();
            // Collect what was in the WALService, if recovering
            Iterator<WALEntry> replay = wal.replay("*");
            while (replay.hasNext()) {
                WALEntry next = replay.next();
                TransactionResult result = new TransactionResult(
                        next.tid, next.timestamp, null, next.vote, next.updates);
                toReplay.add(result);
            }
            double delta = (System.nanoTime() - start) * Math.pow(10, -6);

            MetricAccumulator replayTime = new MetricAccumulator();
            replayTime.add(delta);
            getRuntimeContext().addAccumulator("replay-time-at-sink", replayTime);
            // now that everything is ready we can start the LocalWALServer
            tRuntimeContext.startLocalWALServer();
        }

        transactionCloser = tRuntimeContext.getSinkTransactionCloser();
        transactionCloser.open(wal);

        snapshotService = tRuntimeContext.getSnapshotService(parameterTool);

        getRuntimeContext().addAccumulator("tnx-replays-at-sink", replays);
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
        snapshotService.close();

        if (wal != null) {
            wal.close();
        }

        if (localWALServer != null) {
            localWALServer.close();
        }

        jobControlClient.close();
    }

    private void fillWALs(
            int inputRate, long checkpointIntervalMilliseconds,
            int numberOfSources, RuntimeContext flinkContext) throws IOException {
        int totalNumberOfRecords = Math.round(inputRate * Math.round(checkpointIntervalMilliseconds / 1000));
        int baseNumberOfRecords = totalNumberOfRecords / flinkContext.getNumberOfParallelSubtasks();
        int numberOfRecords = baseNumberOfRecords;

        // The last one gets the remaining records
        if (flinkContext.getIndexOfThisSubtask() == flinkContext.getNumberOfParallelSubtasks() - 1) {
            numberOfRecords += totalNumberOfRecords % flinkContext.getNumberOfParallelSubtasks();
        }
        // I suppose that state is partitioned the same way as this operator
        int statePartitions = flinkContext.getNumberOfParallelSubtasks();

        TimestampGenerator tg[] = new TimestampGenerator[numberOfSources];
        for (int i = 0; i < tg.length; i++) {
            tg[i] = new TimestampGenerator(i, numberOfSources, i);
        }

        int start = baseNumberOfRecords * flinkContext.getIndexOfThisSubtask();
        int end = start + numberOfRecords;
        IntStream.range(start, end)
                .mapToObj(i -> {
                    int index = i % numberOfSources;
                    long nextTS = tg[index].nextTimestamp();
                    long tid = tg[index].toLogical(nextTS);
                    int partition = i % statePartitions;
                    return StateOperator.createFakeTransferEntry(tid, nextTS, partition);
                })
                .forEach(entry -> wal.addEntry(entry));

        // reload the WAL after filling
        wal.forceReload();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<TransactionResult> collector) throws Exception {
        // If simulating, we gathered all the metrics at open and we can kill the job
        if (simulating) {
            jobControlClient.publishFinishMessage();
            return;
        }

        collectReplays(collector);

        if (metadata.vote == Vote.REPLAY) {
            replays.add(1);
        }

        transactionCloser.onMetadata(metadata);

        collector.collect(
                new TransactionResult(
                        metadata.tid,
                        metadata.timestamp,
                        metadata.originalRecord,
                        metadata.vote,
                        metadata.updates)
        );
    }

    private void collectReplays(Collector<TransactionResult> collector) {
        if (!toReplay.isEmpty()) {
            for (TransactionResult result : toReplay) {
                collector.collect(result);
            }
            toReplay.clear();
        }
    }

    // --------------------------------------- Snapshotting

    @Override
    public void notifyCheckpointComplete(long id) throws Exception {
        if (localWALServer != null) {
            localWALServer.commitSnapshot();
            LOG.info("committing snapshot - id " + id);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // does nothing
    }
}
