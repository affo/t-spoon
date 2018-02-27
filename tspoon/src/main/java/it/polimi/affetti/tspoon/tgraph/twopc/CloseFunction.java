package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 18/07/17.
 */
public class CloseFunction extends RichFlatMapFunction<Metadata, TransactionResult> {
    private TRuntimeContext tRuntimeContext;
    private transient AbstractCloseOperatorTransactionCloser transactionCloser;

    // stats
    private IntCounter replays = new IntCounter();

    public CloseFunction(TRuntimeContext tRuntimeContext) {
        this.tRuntimeContext = tRuntimeContext;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        transactionCloser = tRuntimeContext.getSinkTransactionCloser();
        transactionCloser.open();

        getRuntimeContext().addAccumulator("replays-at-sink", replays);
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<TransactionResult> collector) throws Exception {
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
}
