package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.InOrderSideCollector;
import it.polimi.affetti.tspoon.common.RandomProvider;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.query.*;
import it.polimi.affetti.tspoon.tgraph.twopc.CloseTransactionNotification;
import it.polimi.affetti.tspoon.tgraph.twopc.StateOperatorTransactionCloser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private long counter = 0;
    private final String nameSpace;
    public final OutputTag<Update<V>> updatesTag;
    // I suppose that the type for keys is String. This assumption is coherent,
    // for instance, with Redis implementation: https://redis.io/topics/data-types-intro
    protected final Map<String, Object<V>> state;
    protected int maxNumberOfVersions;
    protected StateFunction<T, V> stateFunction;
    // transaction contexts: timestamp -> context
    private Map<Integer, TransactionContext> transactions;

    protected transient InOrderSideCollector<T, Update<V>> collector;

    private transient JobControlClient jobControlClient;

    private transient WithServer srv;
    private transient WithServer queryServer;
    private final StateOperatorTransactionCloser transactionCloser;

    // randomizer to build queries
    private Random random = RandomProvider.get();

    public StateOperator(
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            StateOperatorTransactionCloser transactionCloser) {
        this.nameSpace = nameSpace;
        this.stateFunction = stateFunction;
        this.updatesTag = updatesTag;
        this.transactionCloser = transactionCloser;
        this.state = new ConcurrentHashMap<>();
        this.transactions = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        maxNumberOfVersions = parameterTool.getInt("maxNoVersions", 100);
        jobControlClient = JobControlClient.get(parameterTool);

        srv = new WithServer(new TransactionCloseServer());
        srv.open();

        queryServer = new WithServer(new QueryServer(this));
        queryServer.open();

        if (jobControlClient != null) {
            jobControlClient.registerQueryServer(nameSpace, queryServer.getMyAddress());
        }

        collector = new InOrderSideCollector<>(output, updatesTag);

        transactionCloser.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        srv.close();
        queryServer.close();
        transactionCloser.close();
        if (jobControlClient != null) {
            jobControlClient.close();
        }
    }

    // --------------------------------------- Transaction Execution and Completion ---------------------------------------

    protected synchronized Object<V> getObject(String key) {
        return state.computeIfAbsent(key, k -> new Object<>(stateFunction.defaultValue()));
    }

    @Override
    public void processElement(StreamRecord<Enriched<T>> sr) throws Exception {
        final String key = getCurrentKey().toString();

        T element = sr.getValue().value;
        Metadata metadata = sr.getValue().metadata;

        // do not even process aborted or replayed stuff!
        if (metadata.vote != Vote.COMMIT) {
            collector.safeCollect(Enriched.of(metadata, element));
            return;
        }

        metadata.addCohort(srv.getMyAddress());

        Object<V> object = getObject(key);
        TransactionContext transaction = transactions.computeIfAbsent(metadata.timestamp,
                ts -> {
                    counter++;
                    return new TransactionContext(counter, metadata.tid, ts, metadata.coordinator);
                });
        transaction.addObject(key, object);

        execute(transaction, key, object, metadata, element);
    }

    private int versionCleanup(Object<V> object, int watermark) {
        if (object.getVersionCount() > maxNumberOfVersions) {
            return object.clearVersionsUntil(watermark);
        }
        return 0;
    }

    protected abstract void execute(TransactionContext tContext, String key, Object<V> object, Metadata metadata, T element);

    protected abstract void onTermination(TransactionContext tContext);

    private class TransactionCloseServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(srv.getMyAddress() + " " + request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);

            TransactionContext tContext = transactions.remove(notification.timestamp);
            tContext.vote = notification.vote;
            List<Update<V>> updates = tContext.applyChanges();
            String requestWUpdates = request + "," + updates;

            transactionCloser.closeTransaction(
                    tContext.coordinator, notification.timestamp, requestWUpdates,
                    aVoid -> {
                        collector.collectInOrder(updates, tContext.localId);
                        collector.flushOrdered(tContext.localId);
                        onTermination(tContext);
                    },
                    error -> LOG.error("StateOperator - transaction (" + tContext.tid + ", " + tContext.vote +
                            ") - error on receiving ACK from coordinator: " + error.getMessage())
            );
        }
    }

    public class TransactionContext {
        public final long localId;
        public final int tid;
        // track versions
        public int version;
        private Vote vote;
        // if the same key is edited twice the object is touched only once
        public final Map<String, Object<V>> touchedObjects = new HashMap<>();
        public final Address coordinator;
        Stream<Update<V>> updates;

        public TransactionContext(long localId, int tid, int timestamp, Address coordinator) {
            this.localId = localId;
            this.tid = tid;
            // defaults to timestamp
            this.version = timestamp;
            this.coordinator = coordinator;
        }

        public void addObject(String key, Object<V> object) {
            this.touchedObjects.put(key, object);
        }

        private Stream<Update<V>> calculateUpdates() {
            return touchedObjects.entrySet().stream().map(
                    entry -> Update.of(tid, entry.getKey(),
                            entry.getValue().getVersion(version).object));
        }

        public List<Update<V>> getUpdates() {
            return updates.collect(Collectors.toList());
        }

        public List<Update<V>> applyChanges() {
            // NOTE that commit/abort on multiple objects is not atomic wrt external queries and internal operations
            if (vote == Vote.COMMIT) {
                updates = calculateUpdates();
                for (Object<V> object : touchedObjects.values()) {
                    object.commitVersion(version);
                    // perform version cleanup
                    versionCleanup(object, version);
                }
            } else {
                updates = Stream.empty();
                for (Object<V> object : touchedObjects.values()) {
                    object.deleteVersion(version);
                }
            }

            return updates.collect(Collectors.toList());
        }
    }

    // --------------------------------------- Querying ---------------------------------------

    private Map<String, V> queryState(Iterable<String> keys, int timestamp) {
        Map<String, V> queryResult = new HashMap<>();
        for (String key : keys) {
            V object = getObject(key).getLastVersionBefore(timestamp).object;
            if (object != null) {
                queryResult.put(key, object);
            }
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
        return queryState(query.getKeys(), query.watermark);
    }

    // --------------------------------------- State Recovery ---------------------------------------

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
}
