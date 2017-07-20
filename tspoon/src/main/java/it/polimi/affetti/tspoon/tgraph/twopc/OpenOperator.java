package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.TransactionContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 14/07/17.
 */
public abstract class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>> {
    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };
    private int count;
    private transient WithServer server;
    private transient SafeCollector<T, Integer> collector;

    @Override
    public void open() throws Exception {
        super.open();
        collector = new SafeCollector<>(output, watermarkTag, new StreamRecord<>(null));
        server = new WithServer(getOpenServer(collector));
        server.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        server.close();
    }

    @Override
    public void processElement(StreamRecord<T> sr) throws Exception {
        TransactionContext metadata = getFreshTransactionContext();
        metadata.twoPC.tid = count;
        metadata.twoPC.coordinator = server.getMyAddress();

        Enriched<T> out = Enriched.of(metadata, sr.getValue());

        collector.safeCollect(sr.replace(out));
        count++;
    }

    protected abstract TransactionContext getFreshTransactionContext();

    protected abstract AbstractServer getOpenServer(SafeCollector<T, Integer> collector);
}
