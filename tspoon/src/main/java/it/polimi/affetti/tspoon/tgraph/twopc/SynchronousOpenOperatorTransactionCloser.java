package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.BroadcastByKeyServer;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 09/11/17.
 */
public class SynchronousOpenOperatorTransactionCloser extends AbstractOpenOperatorTransactionCloser {
    private final Map<Integer, Integer> counters = new HashMap<>();

    protected SynchronousOpenOperatorTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    protected AbstractServer getServer() {
        return new OpenServer();
    }

    private synchronized boolean handleStateAck(CloseTransactionNotification notification) {
        int timestamp = notification.timestamp;
        int batchSize = notification.batchSize;

        int count;
        counters.putIfAbsent(timestamp, batchSize);
        count = counters.get(timestamp);
        count--;
        counters.put(timestamp, count);

        if (count == 0) {
            counters.remove(timestamp);
            return true;
        }

        return false;
    }


    private class OpenServer extends BroadcastByKeyServer {
        @Override
        protected void parseRequest(String key, String request) {
            // LOG.info(request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            boolean closed = handleStateAck(notification);
            if (closed) {
                broadcastByKey(key, "");
                notifyListeners(notification, listener -> listener.onCloseTransaction(notification));
            }
        }

        @Override
        protected String extractKey(String request) {
            return request.split(",")[0];
        }
    }
}
