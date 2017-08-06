package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.SafeCollector;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 14/07/17.
 */
public abstract class OpenOperator<T>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<T, Enriched<T>> {
    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };
    public final OutputTag<Tuple2<Integer, Vote>> logTag = new OutputTag<Tuple2<Integer, Vote>>("wal") {
    };
    protected int count;
    private transient WithServer server;
    protected transient SafeCollector<T, Integer> collector;
    private final Map<Integer, Integer> counters = new HashMap<>();
    protected Address myAddress;

    // stats
    protected Map<Vote, IntCounter> stats = new HashMap<>();

    public OpenOperator() {
        for (Vote vote : Vote.values()) {
            stats.put(vote, new IntCounter());
            Report.registerAccumulator(vote.toString().toLowerCase() + "-counter");
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new SafeCollector<>(output, watermarkTag, new StreamRecord<>(null));
        server = new WithServer(new OpenServer());
        server.open();
        myAddress = server.getMyAddress();

        // register accumulators
        for (Map.Entry<Vote, IntCounter> s : stats.entrySet()) {
            Vote vote = s.getKey();
            getRuntimeContext().addAccumulator(vote.toString().toLowerCase() + "-counter", s.getValue());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        server.close();
    }

    @Override
    public void processElement(StreamRecord<T> sr) throws Exception {
        count++;
        Metadata metadata = new Metadata(count);
        metadata.coordinator = myAddress;
        Enriched<T> out = Enriched.of(metadata, sr.getValue());

        openTransaction(out);
        collector.safeCollect(sr.replace(out));
    }

    protected abstract void openTransaction(Enriched<T> element);

    protected abstract void onAck(int timestamp, Vote vote, int replayCause, String updates);

    protected abstract void writeToWAL(int timestamp) throws IOException;

    protected abstract void closeTransaction(int timestamp);

    private class OpenServer extends BroadcastByKeyServer {
        @Override
        protected void parseRequest(String key, String request) {
            // LOG.info(request);

            String[] tokens = request.split(",");
            int timestamp = Integer.parseInt(key);
            Vote vote = Vote.values()[Integer.parseInt(tokens[1])];
            int batchSize = Integer.parseInt(tokens[2]);
            int replayCause = Integer.parseInt(tokens[3]);
            // TODO JSON serialized
            String updates = String.join(",", Arrays.copyOfRange(tokens, 4, tokens.length));

            int count;
            synchronized (counters) {
                counters.putIfAbsent(timestamp, batchSize);
                count = counters.get(timestamp);
                count--;
                if (count == 0) {
                    counters.remove(timestamp);
                } else {
                    counters.put(timestamp, count);
                }
            }

            onAck(timestamp, vote, replayCause, updates);

            if (count == 0) {
                try {
                    writeToWAL(timestamp);
                } catch (IOException e) {
                    // make it crash, we cannot avoid persisting the WAL
                    throw new RuntimeException("Cannot persist to WAL");
                }

                broadcastByKey(key, "");
                closeTransaction(timestamp);

                Tuple2<Integer, Vote> logEntry = Tuple2.of(timestamp, vote);
                collector.safeCollect(logTag, logEntry);
            }
        }

        @Override
        protected String extractKey(String request) {
            return request.split(",")[0];
        }
    }
}
