package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.RandomProvider;
import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.runtime.*;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.query.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by affo on 14/07/17.
 */
public abstract class StateOperator<T, V>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<Enriched<T>, Enriched<T>>,
        QueryVisitor, QueryListener {
    private final String nameSpace;
    public final OutputTag<Update<V>> updatesTag;
    // I suppose that the type for keys is String. This assumption is coherent,
    // for instance, with Redis implementation: https://redis.io/topics/data-types-intro
    protected final Map<String, Object<V>> state;
    protected StateFunction<T, V> stateFunction;
    // list of timestamps ordered by execution order
    protected final List<Integer> executionOrder;
    // transaction contexts: timestamp -> context
    private Map<Integer, TransactionContext> transactions;
    private transient StringClientsCache clientsCache;

    protected transient SafeCollector<T, Update<V>> collector;

    private transient JobControlClient jobControlClient;

    private transient WithServer srv;
    private transient WithServer queryServer;
    private transient ExecutorService pool;

    // randomizer to build queries
    private Random random = RandomProvider.get();

    public StateOperator(String nameSpace, StateFunction<T, V> stateFunction, OutputTag<Update<V>> updatesTag) {
        this.nameSpace = nameSpace;
        this.stateFunction = stateFunction;
        this.updatesTag = updatesTag;
        this.state = new ConcurrentHashMap<>();
        // handle concurrent insertion and deletion in synchronized blocks
        this.executionOrder = new LinkedList<>();
        this.transactions = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        srv = new WithServer(new TransactionCloseServer());
        srv.open();

        queryServer = new WithServer(new QueryServer(this));
        queryServer.open();

        if (jobControlClient != null) {
            jobControlClient.registerQueryServer(nameSpace, queryServer.getMyAddress());
        }

        clientsCache = new StringClientsCache();
        collector = new SafeCollector<>(output, updatesTag, new StreamRecord<>(null));

        pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void close() throws Exception {
        super.close();
        srv.close();
        queryServer.close();
        clientsCache.clear();
        if (jobControlClient != null) {
            jobControlClient.close();
        }
        pool.shutdown();
    }

    protected synchronized Object<V> getObject(String key) {
        return state.computeIfAbsent(key, k -> new Object<>());
    }

    protected void registerExecution(int timestamp) {
        transactions.get(timestamp).registerExecution();
    }

    @Override
    public void processElement(StreamRecord<Enriched<T>> sr) throws Exception {
        final String key = getCurrentKey().toString();

        T element = sr.getValue().value;
        Metadata metadata = sr.getValue().metadata;
        metadata.addCohort(srv.getMyAddress());
        StringClient coordinatorClient = clientsCache.getOrCreateClient(metadata.coordinator);

        Object<V> object = getObject(key);
        TransactionContext transaction = transactions.computeIfAbsent(metadata.timestamp,
                ts -> new TransactionContext(metadata.tid, ts, coordinatorClient));
        transaction.addObject(key, object);
        execute(metadata, key, object, element);
    }

    protected abstract void execute(Metadata metadata, String key, Object<V> object, T element);

    protected abstract void onTermination(int tid, Vote vote);


    private Map<String, V> queryState(Iterable<String> keys, int timestamp) {
        Map<String, V> queryResult = new HashMap<>();
        for (String key : keys) {
            queryResult.put(key, state.get(key).getLastVersionBefore(timestamp).object);
        }

        return queryResult;
    }

    @Override
    public void visit(Query query) {
        // does nothing
    }

    @Override
    public void visit(RandomQuery query) {
        Integer noKeys = state.size();

        if (state.isEmpty()) {
            return;
        }

        Set<Integer> indexes;
        if (noKeys > query.size) {
            indexes = random.ints(0, noKeys).distinct().limit(query.size)
                    .boxed().collect(Collectors.toSet());
        } else {
            indexes = IntStream.range(0, noKeys).boxed().collect(Collectors.toSet());
        }

        int i = 0;
        for (String key : state.keySet()) {
            if (indexes.contains(i)) {
                query.addKey(key);
            }
            i++;
        }
    }

    @Override
    public <U> void visit(PredicateQuery<U> query) {
        for (String key : state.keySet()) {
            V value = state.get(key).getLastVersionBefore(query.watermark).object;
            // hope that the predicate is coherent with the state
            try {
                if (query.test((U) value)) {
                    query.addKey(key);
                }
            } catch (ClassCastException e) {
                LOG.error("Problem with provided predicate...");
            }
        }
    }

    @Override
    public Map<String, ?> onQuery(Query query) {
        query.accept(this);
        Map<String, V> result = queryState(query.getKeys(), query.watermark);

        return result;
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
            // at level PL4 it is non-sense to impose an in-order feedback,
            // because, the StrictnessEnforcer already provides in-order feedback
            // by transaction id.
            if (TransactionEnvironment.isolationLevel != IsolationLevel.PL4) {
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
            }

            // concurrent removals
            StringClient coordinator = tContext.coordinator;

            List<Update<V>> updates = tContext.applyChangesAndGatherUpdates();
            coordinator.send(request + "," + updates);
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

        StringClient coordinator;

        public TransactionContext(int tid, int timestamp, StringClient coordinatorClient) {
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

            // NOTE that commit/abort on multiple objects is not atomic wrt external queries and internal operations
            if (vote == Vote.COMMIT) {
                updates = getUpdates();
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
