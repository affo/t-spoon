package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.InOrderSideCollector;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.BroadcastByKeyServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static it.polimi.affetti.tspoon.tgraph.twopc.TransactionsIndex.LocalTransactionContext;

/**
 * Created by affo on 14/07/17.
 */
public abstract class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>> {
    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };
    public final OutputTag<Tuple2<Long, Vote>> logTag = new OutputTag<Tuple2<Long, Vote>>("wal") {
    };

    private transient WithServer server;
    protected transient InOrderSideCollector<T, Tuple2<Long, Vote>> collector;
    private final Map<Integer, Integer> counters = new HashMap<>();
    protected Address myAddress;

    protected final TransactionsIndex transactionsIndex;
    private transient WAL wal;

    // stats
    protected Map<Vote, IntCounter> stats = new HashMap<>();

    public OpenOperator(TransactionsIndex transactionsIndex) {
        for (Vote vote : Vote.values()) {
            stats.put(vote, new IntCounter());
            Report.registerAccumulator(vote.toString().toLowerCase() + "-counter");
        }

        this.transactionsIndex = transactionsIndex;
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new InOrderSideCollector<>(output, logTag);

        server = new WithServer(new OpenServer());
        server.open();
        myAddress = server.getMyAddress();

        // register accumulators
        for (Map.Entry<Vote, IntCounter> s : stats.entrySet()) {
            Vote vote = s.getKey();
            getRuntimeContext().addAccumulator(vote.toString().toLowerCase() + "-counter", s.getValue());
        }

        // TODO send to kafka
        // up to now, we only introduce overhead by writing to disk
        wal = new DummyWAL("wal.log");
        wal.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        server.close();
        wal.close();
    }

    @Override
    public synchronized void processElement(StreamRecord<T> sr) throws Exception {
        LocalTransactionContext tContext = transactionsIndex.newTransaction();
        Metadata metadata = getMetadataFromContext(tContext);
        onOpenTransaction(sr.getValue(), metadata);
        collector.safeCollect(sr.replace(Enriched.of(metadata, sr.getValue())));
    }

    protected Metadata getMetadataFromContext(LocalTransactionContext tContext) {
        Metadata metadata = new Metadata(tContext.tid);
        metadata.timestamp = tContext.timestamp;
        metadata.coordinator = myAddress;
        metadata.watermark = transactionsIndex.getCurrentWatermark();
        return metadata;
    }

    protected abstract void onOpenTransaction(T recordValue, Metadata metadata);

    private synchronized boolean handleStateAck(CloseTransactionNotification notification) {
        int timestamp = notification.timestamp;
        int batchSize = notification.batchSize;
        int replayCause = notification.replayCause;
        Vote vote = notification.vote;
        String updates = notification.updates;

        int count;
        counters.putIfAbsent(timestamp, batchSize);
        count = counters.get(timestamp);
        count--;
        counters.put(timestamp, count);

        LocalTransactionContext localTransactionContext = transactionsIndex.getTransactionByTimestamp(timestamp);
        localTransactionContext.replayCause = replayCause;
        localTransactionContext.mergeUpdates(updates);
        localTransactionContext.vote = vote;

        if (count == 0) {
            counters.remove(timestamp);

            try {
                writeToWAL(localTransactionContext);
            } catch (IOException e) {
                // make it crash, we cannot avoid persisting the WAL
                throw new RuntimeException("Cannot persist to WAL");
            }

            closeTransaction(localTransactionContext);
            long ts = (long) timestamp;
            collector.collectInOrder(Tuple2.of(ts, vote), ts);
            return true;
        }

        return false;
    }

    // durability
    protected void writeToWAL(LocalTransactionContext tContext) throws IOException {
        int timestamp = tContext.timestamp;
        switch (tContext.vote) {
            case REPLAY:
                wal.replay(timestamp);
                break;
            case ABORT:
                wal.abort(timestamp);
                break;
            default:
                wal.commit(timestamp, tContext.updates);
        }
    }

    protected abstract void closeTransaction(LocalTransactionContext transactionContext);

    private class OpenServer extends BroadcastByKeyServer {
        @Override
        protected void parseRequest(String key, String request) {
            // LOG.info(request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            boolean closed = handleStateAck(notification);
            if (closed) {
                broadcastByKey(key, "");
                collector.flushOrdered(transactionsIndex.getCurrentWatermark());
            }
        }

        @Override
        protected String extractKey(String request) {
            return request.split(",")[0];
        }
    }
}
