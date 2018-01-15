package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.InOrderSideCollector;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.*;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryListener;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.query.QueryServer;
import it.polimi.affetti.tspoon.tgraph.twopc.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by affo on 14/07/17.
 */
public abstract class StateOperator<T, V>
        extends AbstractStreamOperator<Enriched<T>>
        implements OneInputStreamOperator<Enriched<T>, Enriched<T>>,
        StateOperatorTransactionCloseListener, QueryListener {
    private long counter = 0;
    protected final String nameSpace;
    public final OutputTag<Update<V>> updatesTag;
    protected transient Shard<V> shard;
    protected StateFunction<T, V> stateFunction;
    protected final TRuntimeContext tRuntimeContext;
    // timestamp -> localId
    private final Map<Integer, Long> localIds;

    protected transient InOrderSideCollector<T, Update<V>> collector;
    private transient AbstractStateOperatorTransactionCloser transactionCloser;

    public StateOperator(
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            TRuntimeContext tRuntimeContext) {
        this.nameSpace = nameSpace;
        this.stateFunction = stateFunction;
        this.updatesTag = updatesTag;
        this.tRuntimeContext = tRuntimeContext;

        this.localIds = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        int maxNumberOfVersions = parameterTool.getInt("maxNoVersions", 100);
        Object.maxNumberOfVersions = maxNumberOfVersions;

        int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        shard = new Shard<>(nameSpace, taskNumber,
                maxNumberOfVersions, ObjectFunction.fromStateFunction(stateFunction));

        collector = new InOrderSideCollector<>(output, updatesTag);

        transactionCloser = tRuntimeContext.getAtStateTransactionCloser(taskNumber);
        transactionCloser.open();

        if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            transactionCloser.subscribe(this);
        }

        // Querying
        // TODO use openInPool
        QueryServer queryServer = NetUtils.openAsSingleton(NetUtils.ServerType.QUERY,
                () -> new QueryServer(getRuntimeContext()));
        queryServer.listen(this);
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
        NetUtils.closeServerPool(NetUtils.ServerType.QUERY);
    }

    // --------------------------------------- Transaction Execution and Completion ---------------------------------------

    @Override
    public void processElement(StreamRecord<Enriched<T>> sr) throws Exception {
        final String key = getCurrentKey().toString();

        T element = sr.getValue().value;
        Metadata metadata = sr.getValue().metadata;

        metadata.addCohort(transactionCloser.getServerAddress());
        int timestamp = metadata.timestamp;

        boolean newTransaction = shard.addOperation(key, metadata, Operation.from(element, stateFunction));

        if (newTransaction) {
            counter++;
            localIds.put(timestamp, counter);
            if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.SPECIFIC) {
                transactionCloser.subscribeTo(timestamp, this);
            }
        }

        Transaction<V> transaction = shard.getTransaction(timestamp);
        execute(key, sr.getValue(), transaction);
    }

    // hooks
    protected abstract void execute(String key, Enriched<T> record, Transaction<V> transaction) throws Exception;

    protected abstract void onGlobalTermination(Transaction<V> transaction);

    @Override
    public boolean isInterestedIn(long timestamp) {
        return shard.transactionExist((int) timestamp);
    }

    @Override
    public java.lang.Object getMonitorForUpdateLogic() {
        return null;
    }

    @Override
    public void onTransactionClosedSuccess(CloseTransactionNotification notification) {
        Transaction<V> transaction = shard.removeTransaction(notification.timestamp);
        transaction.mergeVote(notification.vote);
        Iterable<Update<V>> updates = transaction.applyChanges();

        long localId = localIds.remove(transaction.timestamp);
        collector.collectInOrder(updates, localId);
        collector.flushOrdered(localId);

        onGlobalTermination(transaction);
    }

    @Override
    public void onTransactionClosedError(
            CloseTransactionNotification notification, Throwable error) {
        Transaction<V> transaction = shard.removeTransaction(notification.timestamp);
        LOG.error("StateOperator - transaction [" + transaction.tid +
                "] - error on transaction close: " + error.getMessage());
    }

    @Override
    public String getUpdatesRepresentation(int timestamp) {
        return shard.getTransaction(timestamp).getUpdates().toString();
    }

    @Override
    public Address getCoordinatorAddressForTransaction(int timestamp) {
        return shard.getTransaction(timestamp).getCoordinator();
    }

    // --------------------------------------- Querying ---------------------------------------

    @Override
    public QueryResult onQuery(Query query) {
        return shard.runQuery(query);
    }

    @Override
    public Iterable<String> getNameSpaces() {
        return Collections.singleton(nameSpace);
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
