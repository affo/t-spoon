package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.durability.FileWAL;
import it.polimi.affetti.tspoon.tgraph.durability.LocalWALServer;
import it.polimi.affetti.tspoon.tgraph.durability.SnapshotService;
import it.polimi.affetti.tspoon.tgraph.durability.WALEntry;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    private boolean restored;

    private List<TransactionResult> toReplay;

    // stats
    private IntCounter replays = new IntCounter();

    public CloseFunction(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
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

        if (tRuntimeContext.isDurabilityEnabled()) {
            // TODO this is dirty...
            int numberOfWals = getRuntimeContext().getNumberOfParallelSubtasks() / tRuntimeContext.getTaskManagers().length;
            localWALServer = tRuntimeContext.getLocalWALServer(numberOfWals, parameterTool);
            String walName = String.format("tg%d_%d", tRuntimeContext.getGraphId(), getRuntimeContext().getIndexOfThisSubtask());
            wal = new FileWAL(walName, !restored);
            wal.open();
            localWALServer.addWAL(wal);

            MetricAccumulator loadWALTime = new MetricAccumulator();
            loadWALTime.add((double) wal.getLoadWALTime());

            getRuntimeContext().addAccumulator("load-wal-time", loadWALTime);

            // Collect what was in the WALService, if recovering
            Iterator<WALEntry> replay = wal.replay("*");
            while (replay.hasNext()) {
                WALEntry next = replay.next();
                TransactionResult result = new TransactionResult(
                        next.tid, next.timestamp, null, next.vote, next.updates);
                toReplay.add(result);
            }
        }

        transactionCloser = tRuntimeContext.getSinkTransactionCloser();
        transactionCloser.open(wal);

        snapshotService = tRuntimeContext.getSnapshotService(parameterTool);

        getRuntimeContext().addAccumulator("replays-at-sink", replays);
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
        snapshotService.close();
        wal.close();
        if (localWALServer != null) {
            localWALServer.close();
        }
    }

    @Override
    public void flatMap(Metadata metadata, Collector<TransactionResult> collector) throws Exception {
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
