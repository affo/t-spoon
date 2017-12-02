package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.BroadcastByKeyServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 09/11/17.
 */
public class DurableCoordinatorTransactionCloser implements CoordinatorTransactionCloser {
    private CoordinatorCloseTransactionListener listener;
    private transient WithServer server;
    private final Map<Integer, Integer> counters = new HashMap<>();
    private final Map<Integer, String> updates = new HashMap<>();

    private transient WAL wal;

    @Override
    public void open(CoordinatorCloseTransactionListener listener) throws Exception {
        this.listener = listener;

        server = new WithServer(new OpenServer());
        server.open();

        // TODO send to kafka
        // up to now, we only introduce overhead by writing to disk
        wal = new DummyWAL("wal.log");
        wal.open();
    }

    @Override
    public void close() throws Exception {
        this.server.close();
    }

    public Address getAddress() {
        return server.getMyAddress();
    }

    private synchronized boolean handleStateAck(CloseTransactionNotification notification) {
        int timestamp = notification.timestamp;
        int batchSize = notification.batchSize;
        Vote vote = notification.vote;

        int count;
        counters.putIfAbsent(timestamp, batchSize);
        count = counters.get(timestamp);
        count--;
        counters.put(timestamp, count);

        String updates = this.updates.getOrDefault(timestamp, "");
        updates += notification.updates;
        this.updates.put(timestamp, updates);

        if (count == 0) {
            counters.remove(timestamp);

            try {
                writeToWAL(timestamp, vote, this.updates.remove(timestamp));
            } catch (IOException e) {
                // make it crash, we cannot avoid persisting the WAL
                throw new RuntimeException("Cannot persist to WAL");
            }

            return true;
        }

        return false;
    }

    protected void writeToWAL(int timestamp, Vote vote, String updates) throws IOException {
        switch (vote) {
            case REPLAY:
                wal.replay(timestamp);
                break;
            case ABORT:
                wal.abort(timestamp);
                break;
            default:
                wal.commit(timestamp, updates);
        }
    }

    private class OpenServer extends BroadcastByKeyServer {
        @Override
        protected void parseRequest(String key, String request) {
            // LOG.info(request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            boolean closed = handleStateAck(notification);
            if (closed) {
                broadcastByKey(key, "");
                listener.onCloseTransaction(notification);
            }
        }

        @Override
        protected String extractKey(String request) {
            return request.split(",")[0];
        }
    }
}
