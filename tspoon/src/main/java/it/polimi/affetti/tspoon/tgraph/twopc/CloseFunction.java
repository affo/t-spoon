package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
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
        implements CheckpointListener {
    private transient Logger LOG;
    private TRuntimeContext tRuntimeContext;
    private transient WAL wal;
    private transient AbstractCloseOperatorTransactionCloser transactionCloser;

    private List<TransactionResult> toReplay;

    // stats
    private IntCounter replays = new IntCounter();

    public CloseFunction(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
        this.toReplay = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        transactionCloser = tRuntimeContext.getSinkTransactionCloser();
        wal = tRuntimeContext.getWALFactory().getWAL(parameterTool);
        transactionCloser.open(wal);

        // only the first task collects everything that is in the WAL,
        // because it doen't know if it was processed downstream...
        if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
            Iterator<WAL.Entry> replay = wal.replay("*");
            while (replay.hasNext()) {
                WAL.Entry next = replay.next();
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
        wal.commitSnapshot();
        LOG.info("committing snapshot - id " + id);
    }
}
