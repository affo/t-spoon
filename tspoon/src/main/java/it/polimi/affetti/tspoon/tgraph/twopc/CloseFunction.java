package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.durability.LocalWALService;
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
    private transient LocalWALService localWALService;
    private transient AbstractCloseOperatorTransactionCloser transactionCloser;

    private List<TransactionResult> toReplay;

    // stats
    private IntCounter replays = new IntCounter();

    public CloseFunction(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
        this.toReplay = new ArrayList<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        boolean restored = functionInitializationContext.isRestored();
        // could be null
        localWALService = tRuntimeContext.getLocalWALServer(restored);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        transactionCloser = tRuntimeContext.getSinkTransactionCloser();
        transactionCloser.open(localWALService);

        snapshotService = tRuntimeContext.getSnapshotService(parameterTool);

        // Collect what was in the WAL, if recovering
        if (localWALService != null) {
            Iterator<WALEntry> replay = localWALService.replay("*");
            while (replay.hasNext()) {
                WALEntry next = replay.next();
                TransactionResult result = new TransactionResult(
                        next.tid, next.timestamp, null, next.vote, next.updates);
                toReplay.add(result);
            }
        }

        getRuntimeContext().addAccumulator("replays-at-sink", replays);
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
        snapshotService.close();
        if (localWALService != null) {
            localWALService.close();
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
        if (localWALService != null) {
            localWALService.commitSnapshot();
            LOG.info("committing snapshot - id " + id);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // does nothing
    }
}
