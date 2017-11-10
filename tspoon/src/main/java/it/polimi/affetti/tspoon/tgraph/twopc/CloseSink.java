package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 18/07/17.
 */
public class CloseSink extends RichSinkFunction<Metadata> {
    private CloseSinkTransactionCloser transactionCloser;

    public CloseSink(CloseSinkTransactionCloser transactionCloser) {
        this.transactionCloser = transactionCloser;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        transactionCloser.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
    }

    @Override
    public void invoke(Metadata metadata) throws Exception {
        transactionCloser.onMetadata(metadata);
    }
}
