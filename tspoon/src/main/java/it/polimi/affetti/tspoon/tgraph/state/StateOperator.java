package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.common.TaskExecutor;
import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.*;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.twopc.*;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by affo on 14/07/17.
 */
public abstract class StateOperator<T, V>
        extends AbstractStreamOperator<Enriched<T>>
        implements TwoInputStreamOperator<Enriched<T>, NoConsensusOperation, Enriched<T>>,
        StateOperatorTransactionCloseListener, Object.DeferredReadListener {
    protected final int tGraphID;
    protected final String nameSpace;
    public final OutputTag<QueryResult> queryResultTag = new OutputTag<QueryResult>("queryResult") {
    };
    public final OutputTag<TransactionResult> singlePartitionTag = new OutputTag<TransactionResult>("singlePartition") {
    };
    protected transient Shard<V> shard;
    protected final StateFunction<T, V> stateFunction;
    protected final KeySelector<T, String> keySelector;
    protected final TRuntimeContext tRuntimeContext;

    private IntCounter inconsistenciesPrevented = new IntCounter();
    private MetricAccumulator recoveryTime = new MetricAccumulator();

    protected transient SafeCollector<T> collector;
    private transient AbstractStateOperatorTransactionCloser transactionCloser;
    private transient TaskExecutor singlePartitionOperationExecutor;
    private transient WAL wal;

    public StateOperator(
            int tGraphID,
            String nameSpace,
            StateFunction<T, V> stateFunction,
            KeySelector<T, String> ks,
            TRuntimeContext tRuntimeContext) {
        this.tGraphID = tGraphID;
        this.nameSpace = nameSpace;
        this.stateFunction = stateFunction;
        this.keySelector = ks;
        this.tRuntimeContext = tRuntimeContext;
    }

    @Override
    public void open() throws Exception {
        super.open();
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // TODO by default, we disable version compaction (for now...).
        int maxNumberOfVersions = parameterTool.getInt("maxNoVersions", Integer.MAX_VALUE);

        int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        int numberOfTasks = getRuntimeContext().getNumberOfParallelSubtasks();

        wal = tRuntimeContext.getWALFactory().getWAL(parameterTool);
        wal.open();

        shard = new Shard<>(
                nameSpace, taskNumber, numberOfTasks, maxNumberOfVersions,
                tRuntimeContext.getIsolationLevel().gte(IsolationLevel.PL2), wal,
                ObjectFunction.fromStateFunction(stateFunction));

        if (tRuntimeContext.needWaitOnRead()) {
            shard.forceSerializableRead();
            shard.setDeferredReadsListener(this);
        }

        if (tRuntimeContext.isDurabilityEnabled()) {
            getRuntimeContext().addAccumulator("recovery-time", recoveryTime);
        }

        long start = System.nanoTime();
        initState();
        double delta = (System.nanoTime() - start) / Math.pow(10, 6); // ms
        recoveryTime.add(delta);


        collector = new SafeCollector<>(output);

        transactionCloser = tRuntimeContext.getAtStateTransactionCloser(taskNumber);
        transactionCloser.open();

        if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            transactionCloser.subscribe(this);
        }

        singlePartitionOperationExecutor = new TaskExecutor();
        singlePartitionOperationExecutor.start();

        if (tRuntimeContext.needWaitOnRead()) {
            getRuntimeContext().addAccumulator("inconsistencies-prevented", inconsistenciesPrevented);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        transactionCloser.close();
        singlePartitionOperationExecutor.interrupt();
        wal.close();
        NetUtils.closeServerPool(NetUtils.ServerType.QUERY);
    }

    // --------------------------------------- Transaction Execution and Completion ---------------------------------------

    @Override
    public void processElement1(StreamRecord<Enriched<T>> sr) throws Exception {
        T element = sr.getValue().value;
        Metadata metadata = sr.getValue().metadata;
        final String key = keySelector.getKey(element);

        metadata.addCohort(transactionCloser.getServerAddress());
        int timestamp = metadata.timestamp;

        boolean newTransaction = shard.addOperation(key, metadata, Operation.from(element, stateFunction));

        if (newTransaction) {
            if (tRuntimeContext.getSubscriptionMode() == AbstractTwoPCParticipant.SubscriptionMode.SPECIFIC) {
                transactionCloser.subscribeTo(timestamp, this);
            }
        }

        Transaction<V> transaction = shard.getTransaction(timestamp);

        execute(key, sr.getValue(), transaction);
    }

    @Override
    public void processElement2(StreamRecord<NoConsensusOperation> streamRecord) throws Exception {
        singlePartitionOperationExecutor.addTask(() -> {
            NoConsensusOperation noConsensusOperation = streamRecord.getValue();

            if (noConsensusOperation.isReadOnly()) {
                processQuery(noConsensusOperation.getReadOnly());
            } else {
                processSinglePartitionUpdate(noConsensusOperation.getUpdate());
            }
        });
    }

    private void processQuery(Query query) {
        QueryResult result = shard.runQuery(query);
        collector.safeCollect(queryResultTag, result);
    }

    private void processSinglePartitionUpdate(SinglePartitionUpdate update)
            throws InvocationTargetException, IllegalAccessException {
        TransactionResult result = shard.runSinglePartitionUpdate(update);
        collector.safeCollect(singlePartitionTag, result);
    }

    @Override
    public void onDeferredExecution() {
        inconsistenciesPrevented.add(1);
    }

    // hooks
    protected abstract void execute(String key, Enriched<T> record, Transaction<V> transaction) throws Exception;

    protected abstract void onGlobalTermination(Transaction<V> transaction);

    @Override
    public boolean isInterestedIn(long timestamp) {
        return shard.transactionExist((int) timestamp);
    }

    @Override
    public int getTGraphID() {
        return tGraphID;
    }

    @Override
    public void onTransactionClosedSuccess(CloseTransactionNotification notification) {
        Transaction<V> transaction = shard.removeTransaction(notification.timestamp);
        transaction.mergeVote(notification.vote);
        transaction.applyChanges();

        onGlobalTermination(transaction);
    }

    @Override
    public void onTransactionClosedError(
            CloseTransactionNotification notification, Throwable error) {
        Transaction<V> transaction = shard.removeTransaction(notification.timestamp);
        String errorMessage = "StateOperator - transaction [" + transaction.tid +
                "] - error on transaction close: " + error.getMessage();
        LOG.error(errorMessage);
        // errors on closing transactions must not happen
        throw new RuntimeException(new Exception(errorMessage, error));
    }

    @Override
    public String getUpdatesRepresentation(int timestamp) {
        return shard.getTransaction(timestamp).getUpdates().toString();
    }

    @Override
    public Address getCoordinatorAddressForTransaction(int timestamp) {
        return shard.getTransaction(timestamp).getCoordinator();
    }

    /** TODO review
     * This method is an helper for subclasses for enriching a record with the updates of some transaction for
     * some key.
     *
     * It expects a record that is ready for collection (dependency tracking done and merged vote) and a transaction
     * that has already registered updates (versions) for key `key`.
     *
     * It produces some side-effect only in the case that the record is committing, durability is enabled and we
     * are using the ASYNCH protocol (SYNCH protocol gathers updates onSinkACK).
     * @param key
     * @param record
     * @param transaction
     */
    protected void decorateRecordWithUpdates(String key, Enriched<T> record, Transaction<V> transaction) {
        if (record.metadata.vote == Vote.COMMIT &&
                tRuntimeContext.isDurabilityEnabled() && !tRuntimeContext.isSynchronous()) {
            V version = transaction.getVersion(key);
            record.metadata.addUpdate(nameSpace, key, version);
        }
    }

    // --------------------------------------- State Recovery & Snapshotting ---------------------------------------

    private ListState<Map<String, V>> snapshot;

    /**
     * Stream operators with state, which want to participate in a snapshot need to override this hook method.
     *
     * (affo: This operation should be performed in isolation wrt processElement1 and processElement2)
     * @param context context that provides information and means required for taking a snapshot
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        int wm = wal.getSnapshotInProgressWatermark();
        Map<String, V> snapshot = shard.getConsistentSnapshot(wm);
        this.snapshot.clear();
        this.snapshot.add(snapshot);
    }

    /**
     * Stream operators with state which can be restored need to override this hook method.
     *
     * @param context context that allows to register different states.
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        TypeInformation<Map<String, V>> stateType = TypeInformation.of(new TypeHint<Map<String, V>>() {
        });

        // recover snapshot
        snapshot = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("snapshot", stateType));
    }

    /**
     * Called on `open`, after that `wal` and `shard` are created.
     * If not restoring, then the snapshot and the WAL are empty.
     *
     * @throws Exception
     */
    private void initState() throws Exception {
        Map<String, V> snapshot = null;

        for (Map<String, V> snap : this.snapshot.get()) {
            snapshot = snap;
        }

        if (snapshot != null) {
            shard.installSnapshot(snapshot);
        }

        // replay WAL
        Iterator<WAL.Entry> walIterator = wal.replay(nameSpace);
        while (walIterator.hasNext()) {
            WAL.Entry entry = walIterator.next();
            if (entry.vote == Vote.COMMIT) {
                Map<String, java.lang.Object> myUpdates = entry.updates.getUpdatesFor(nameSpace);
                for (Map.Entry<String, java.lang.Object> update : myUpdates.entrySet()) {
                    shard.recover(update.getKey(), (V) update.getValue());
                }
            }
        }

        shard.signalRecoveryComplete();
    }
}
