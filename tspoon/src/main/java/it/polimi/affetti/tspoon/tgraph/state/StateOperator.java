package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.TextClient;
import it.polimi.affetti.tspoon.runtime.TextClientsCache;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.query.QueryTuple;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by affo on 14/07/17.
 */
public abstract class StateOperator<T, V>
        extends AbstractStreamOperator<Enriched<T>>
        implements TwoInputStreamOperator<Enriched<T>, QueryTuple, Enriched<T>> {
    public final OutputTag<Update<V>> updatesTag;
    // I suppose that the type for keys is String. This assumption is coherent,
    // for instance, with Redis implementation: https://redis.io/topics/data-types-intro
    protected Map<String, Object<V>> state;
    protected StateFunction<T, V> stateFunction;
    // list of timestamps ordered by execution order
    protected final List<Integer> executionOrder;
    // transaction contexts: timestamp -> context
    private Map<Integer, TransactionContext> transactions;
    private transient TextClientsCache clientsCache;

    protected transient SafeCollector<T, Update<V>> collector;

    private transient WithServer srv;
    private transient ExecutorService pool;

    public StateOperator(StateFunction<T, V> stateFunction, OutputTag<Update<V>> updatesTag) {
        this.stateFunction = stateFunction;
        this.updatesTag = updatesTag;
        // custom keys-level concurrency control
        this.state = new HashMap<>();
        // handle concurrent insertion and deletion
        this.executionOrder = new LinkedList<>();
        this.transactions = new HashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        srv = new WithServer(new TransactionCloseServer());
        srv.open();

        clientsCache = new TextClientsCache();
        collector = new SafeCollector<>(output, updatesTag, new StreamRecord<>(null));
        pool = Executors.newCachedThreadPool();
    }

    @Override
    public void close() throws Exception {
        super.close();
        srv.close();
        clientsCache.clear();
        pool.shutdown();
    }

    protected synchronized Object<V> getObject(String key) {
        return state.computeIfAbsent(key, k -> new Object<>());
    }

    protected void registerExecution(int timestamp) {
        transactions.get(timestamp).registerExecution();
    }

    @Override
    public void processElement1(StreamRecord<Enriched<T>> sr) throws Exception {
        final String key = getCurrentKey().toString();

        T element = sr.getValue().value;
        Metadata metadata = sr.getValue().metadata;
        metadata.addCohort(srv.getMyAddress());
        TextClient coordinatorClient = clientsCache.getOrCreateClient(metadata.coordinator);

        Object<V> object = getObject(key);
        TransactionContext transaction = transactions.computeIfAbsent(metadata.timestamp,
                ts -> new TransactionContext(metadata.tid, ts, coordinatorClient));
        transaction.addObject(key, object);
        execute(metadata, key, object, element);
    }

    protected abstract void execute(Metadata metadata, String key, Object<V> object, T element);

    protected abstract void onTermination(int tid, Vote vote);

    @Override
    public void processElement2(StreamRecord<QueryTuple> sr) throws Exception {
        // TODO implement querying... getUpdates
    }

    // TODO checkpoint consistent snapshot
    // use Object.getLastCommittedVersion

    /**
     * Stream operators with state, which want to participate in a snapshot need to override this hook method.
     *
     * @param context context that provides information and means required for taking a snapshot
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    /**
     * Stream operators with state which can be restored need to override this hook method.
     *
     * @param context context that allows to register different states.
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
    }


    private void onTransactionClose(TransactionContext tContext, String request) {
        int timestamp = tContext.timestamp;

        // impose in-order feedback in asynchronous task
        pool.submit(() -> {
            synchronized (executionOrder) {
                while (!executionOrder.get(0).equals(timestamp)) {
                    try {
                        executionOrder.wait();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while waiting to send back to coordinator");
                    }
                }

                executionOrder.remove(0);
                executionOrder.notifyAll();
            }

            // concurrent removals
            TextClient coordinator = tContext.coordinator;

            List<Update<V>> updates = tContext.applyChangesAndGatherUpdates();
            coordinator.text(request + "," + updates);
            try {
                String text = coordinator.receive();
                updates.forEach(collector::safeCollect);
            } catch (IOException e) {
                LOG.error("Error on updates collection: " + e.getMessage());
            }

            onTermination(tContext.tid, tContext.vote);
        });
    }

    private class TransactionCloseServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(srv.getMyAddress() + " " + request);

            String[] tokens = request.split(",");
            int timestamp = Integer.parseInt(tokens[0]);
            Vote vote = Vote.values()[Integer.parseInt(tokens[1])];

            TransactionContext tContext = transactions.remove(timestamp);
            tContext.vote = vote;
            onTransactionClose(tContext, request);
        }
    }

    protected class TransactionContext {
        int tid;
        // track versions
        int timestamp;
        Vote vote;
        // if the same key is edited twice the object is touched only once
        Map<String, Object<V>> touchedObjects = new HashMap<>();
        private boolean registered;

        TextClient coordinator;

        public TransactionContext(int tid, int timestamp, TextClient coordinatorClient) {
            this.tid = tid;
            this.timestamp = timestamp;
            this.coordinator = coordinatorClient;
        }

        public void addObject(String key, Object<V> object) {
            this.touchedObjects.put(key, object);
        }

        public Stream<Update<V>> getUpdates() {
            return touchedObjects.entrySet().stream().map(
                    entry -> Update.of(tid, entry.getKey(), entry.getValue().getLastVersionBefore(timestamp).object));
        }

        public void registerExecution() {
            if (!registered) {
                synchronized (executionOrder) {
                    executionOrder.add(timestamp);
                }
                registered = true;
            }
        }

        public List<Update<V>> applyChangesAndGatherUpdates() {
            Stream<Update<V>> updates;

            if (vote == Vote.COMMIT) {
                updates = getUpdates();
                for (Object<V> object : touchedObjects.values()) {
                    object.commit(timestamp);
                }
            } else {
                updates = Stream.empty();
                for (Object<V> object : touchedObjects.values()) {
                    object.deleteVersion(timestamp);
                }
            }

            return updates.collect(Collectors.toList());
        }

    }
}
