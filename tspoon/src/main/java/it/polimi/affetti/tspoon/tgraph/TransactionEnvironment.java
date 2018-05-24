package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.ControlledSource;
import it.polimi.affetti.tspoon.evaluation.EvalConfig;
import it.polimi.affetti.tspoon.tgraph.query.*;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.twopc.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static it.polimi.affetti.tspoon.tgraph.IsolationLevel.PL0;
import static it.polimi.affetti.tspoon.tgraph.IsolationLevel.PL3;

/**
 * Created by affo on 17/07/17.
 * <p>
 * Represents the transactional graph context
 */
public class TransactionEnvironment {
    private static TransactionEnvironment instance;

    private final StreamExecutionEnvironment streamExecutionEnvironment;
    private boolean isDurabilityEnabled = false;
    private DataStream<MultiStateQuery> queryStream;
    private SplitStream<SinglePartitionUpdate> spuStream;
    private QueryResultMerger.OnQueryResult onQueryResult = new QueryResultMerger.NOPOnQueryResult();
    private TwoPCFactory factory;
    private IsolationLevel isolationLevel = PL3; // max level by default
    private Strategy strategy;
    private boolean useDependencyTracking = true;
    private boolean verbose = false;
    // Pool sizes are per TaskManager (e.g. stateServerPoolSize = 4 and 3 TMs => 12 StateServers)
    // Pool sizes defaults to singletons.
    private int stateServerPoolSize = 1, openServerPoolSize = 1, queryServerPoolSize = 1;
    private boolean synchronous;
    private boolean baselineMode;
    private String[] taskManagers;

    private int tGraphId = 0;
    private Map<Integer, DataStream<TransactionResult>> spuResultsPerTGraph = new HashMap<>();
    private String sourcesSharingGroup = "default";
    private int sourcesParallelism;
    private int closeBatchSize;

    private TransactionEnvironment(StreamExecutionEnvironment env) {
        this.streamExecutionEnvironment = env;
    }

    public synchronized static TransactionEnvironment get(StreamExecutionEnvironment env) {
        if (instance == null) {
            instance = new TransactionEnvironment(env);
            instance.registerCustomSerializers();
            instance.sourcesParallelism = env.getParallelism();
        }
        return instance;
    }

    public synchronized static TransactionEnvironment fromConfig(EvalConfig config) throws IOException {
        if (instance == null) {
            instance = new TransactionEnvironment(config.getFlinkEnv());
            instance.registerCustomSerializers();
            instance.sourcesParallelism = instance.streamExecutionEnvironment.getParallelism();
            instance.configIsolation(config.strategy, config.isolationLevel);
            instance.setUseDependencyTracking(config.useDependencyTracking);
            instance.setSynchronous(config.synchronous);
            instance.setVerbose(false);
            instance.setOpenServerPoolSize(config.openServerPoolSize);
            instance.setStateServerPoolSize(config.stateServerPoolSize);
            instance.setQueryServerPoolSize(config.queryServerPoolSize);
            instance.setBaselineMode(config.baselineMode);
            instance.setSourcesSharingGroup(EvalConfig.sourceSharingGroup, config.sourcePar);
            instance.setTaskManagers(config.taskManagerIPs);
            instance.setCloseBatchSize(config.closeBatchSize);

            if (config.durable) {
                instance.enableDurability();
            }
        }
        return instance;
    }

    private void registerCustomSerializers() {
        streamExecutionEnvironment.getConfig().enableForceKryo();
        // known bug: https://issues.apache.org/jira/browse/FLINK-6025
        streamExecutionEnvironment.getConfig().registerTypeWithKryoSerializer(Address.class, JavaSerializer.class);
        streamExecutionEnvironment.getConfig().registerTypeWithKryoSerializer(BatchID.class, JavaSerializer.class);
        streamExecutionEnvironment.getConfig().registerTypeWithKryoSerializer(Metadata.class, JavaSerializer.class);
        streamExecutionEnvironment.getConfig().registerTypeWithKryoSerializer(Enriched.class, JavaSerializer.class);
    }

    // only to run 2 jobs
    public synchronized static void clear() {
        instance = null;
    }

    public void enableStandardQuerying(QuerySupplier querySupplier) {
        this.enableStandardQuerying(querySupplier, sourcesParallelism);
    }

    public void enableStandardQuerying(QuerySupplier querySupplier, int queryPar) {
        Preconditions.checkState(this.queryStream == null, "Cannot enable querying more than once");

        QuerySource querySource = new QuerySource();
        querySource.setQuerySupplier(querySupplier);
        this.queryStream = streamExecutionEnvironment
                .addSource(querySource)
                .name("QuerySource")
                .slotSharingGroup(sourcesSharingGroup)
                .setParallelism(queryPar);
    }

    public void enableSPUpdates(SingleOutputStreamOperator<SinglePartitionUpdate> spuStream) {
        Preconditions.checkState(this.spuStream == null, "Cannot enable querying more than once");

        spuStream = spuStream
                .slotSharingGroup(sourcesSharingGroup)
                .setParallelism(sourcesParallelism);

        this.spuStream = spuStream.split(spu -> Collections.singleton(spu.nameSpace));
    }

    public void enableCustomQuerying(DataStream<MultiStateQuery> queryStream) {
        Preconditions.checkState(this.queryStream == null, "Cannot enable querying more than once");

        this.queryStream = queryStream;
    }

    public void setOnQueryResult(QueryResultMerger.OnQueryResult onQueryResult) {
        this.onQueryResult = onQueryResult;
    }

    public QueryResultMerger.OnQueryResult getOnQueryResult() {
        return onQueryResult;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void configIsolation(Strategy strategy, IsolationLevel isolationLevel) {
        if (isolationLevel == PL0) {
            // PL0 can be provided only with optimistic strategy
            strategy = Strategy.OPTIMISTIC;
        }

        this.strategy = strategy;
        switch (strategy) {
            case PESSIMISTIC:
                this.factory = new PessimisticTwoPCFactory();
                break;
            default:
                this.factory = new OptimisticTwoPCFactory();
        }

        this.isolationLevel = isolationLevel;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public void setUseDependencyTracking(boolean useDependencyTracking) {
        this.useDependencyTracking = useDependencyTracking;
    }

    public boolean usingDependencyTracking() {
        return useDependencyTracking;
    }

    public void enableDurability() throws IOException {
        isDurabilityEnabled = true;
        streamExecutionEnvironment.enableCheckpointing(60000);
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                60, Time.of(10, TimeUnit.SECONDS)));
        streamExecutionEnvironment.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));
    }

    public void setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
    }

    public boolean isSynchronous() {
        return synchronous;
    }

    public boolean isDurabilityEnabled() {
        return isDurabilityEnabled;
    }

    public int getStateServerPoolSize() {
        return stateServerPoolSize;
    }

    public void setStateServerPoolSize(int stateServerPoolSize) {
        this.stateServerPoolSize = stateServerPoolSize;
    }

    public int getOpenServerPoolSize() {
        return openServerPoolSize;
    }

    public void setOpenServerPoolSize(int openServerPoolSize) {
        this.openServerPoolSize = openServerPoolSize;
    }

    public int getQueryServerPoolSize() {
        return queryServerPoolSize;
    }

    public void setQueryServerPoolSize(int queryServerPoolSize) {
        this.queryServerPoolSize = queryServerPoolSize;
    }

    public void setBaselineMode(boolean baselineMode) {
        this.baselineMode = baselineMode;
    }

    public boolean isBaselineMode() {
        return baselineMode;
    }

    /**
     *
     * @param sourcesSharingGroup "default" if no separation
     * @param sourcePar <= 0 if you want to preserve global setting
     */
    public void setSourcesSharingGroup(String sourcesSharingGroup, int sourcePar) {
        this.sourcesSharingGroup = sourcesSharingGroup;
        if (sourcePar > 0) {
            this.sourcesParallelism = sourcePar;
        }
    }

    public int getSourcesParallelism() {
        return sourcesParallelism;
    }

    public String getSourcesSharingGroup() {
        return sourcesSharingGroup;
    }

    public TRuntimeContext createTransactionalRuntimeContext(int tGraphId) {
        TRuntimeContext runtimeContext = new TRuntimeContext(tGraphId);
        runtimeContext.setSynchronous(synchronous);
        runtimeContext.setDurabilityEnabled(isDurabilityEnabled);
        runtimeContext.setIsolationLevel(isolationLevel);
        runtimeContext.setUseDependencyTracking(useDependencyTracking);
        runtimeContext.setStrategy(strategy);
        runtimeContext.setOpenServerPoolSize(openServerPoolSize);
        runtimeContext.setStateServerPoolSize(stateServerPoolSize);
        runtimeContext.setQueryServerPoolSize(queryServerPoolSize);
        runtimeContext.setBaselineMode(baselineMode);
        runtimeContext.setTaskManagers(taskManagers);
        runtimeContext.setCloseBatchSize(closeBatchSize);
        return runtimeContext;
    }

    public SplitStream<SinglePartitionUpdate> getSpuStream() {
        Preconditions.checkState(this.spuStream != null,
                "Cannot get SPUStream if not set: " + this.spuStream);

        return spuStream;
    }

    public void addSPUResults(int tGraphID, DataStream<TransactionResult> spuResults) {
        DataStream<TransactionResult> results = spuResultsPerTGraph.get(tGraphID);
        if (results == null) {
            results = spuResults;
        } else {
            results = results.union(spuResults);
        }
        spuResultsPerTGraph.put(tGraphID, results);
    }

    // ------------------- Open/Close

    public <T> OpenStream<T> open(DataStream<T> ds) {
        AbstractTStream.setTransactionEnvironment(this);

        if (queryStream == null) {
            enableStandardQuerying(new NullQuerySupplier());
        }

        if (spuStream == null) {
            SingleOutputStreamOperator<SinglePartitionUpdate> spuStream = streamExecutionEnvironment
                    .addSource(new EmptySPUSource())
                    .name("SPUSource");
            enableSPUpdates(spuStream);
        }

        return factory.open(ds, queryStream, tGraphId++);
    }

    public DataStream<TransactionResult> close(TStream<?>... exitPoints) {
        int n = exitPoints.length;
        assert n >= 1;

        int tGraphID = exitPoints[0].getTGraphID();
        for (TStream<?> exitPoint : exitPoints) {
            assert exitPoint.getTGraphID() == tGraphID;
        }

        List<DataStream<Metadata>> firstStepMerged = new ArrayList<>(n);
        for (TStream<?> exitPoint : exitPoints) {
            DataStream<? extends Enriched<?>> enclosed = exitPoint.getEnclosingStream();
            // first step reduction of votes on each exit point
            DataStream<Metadata> twoPC = enclosed.map(new MetadataExtractor<>());
            DataStream<Metadata> reduced = twoPC
                    .keyBy(tpc -> tpc.timestamp)
                    .flatMap(new ReduceVotesFunction())
                    .name("FirstStepReduceVotes");
            firstStepMerged.add(reduced);
        }

        // second step reduction on every exit point using a batch size
        // equal to the number of exit points
        List<DataStream<Metadata>> withLastStep = IntStream.range(0, n)
                .mapToObj(index ->
                        firstStepMerged.get(index)
                                .map(new LastStepAdder(index + 1, n)))
                .collect(Collectors.toList());
        DataStream<Metadata> union = withLastStep.get(0);

        for (DataStream<Metadata> exitPoint : withLastStep.subList(1, n)) {
            union = union.union(exitPoint);
        }

        DataStream<Metadata> secondMerged = union
                .keyBy(m -> m.timestamp)
                .flatMap(new ReduceVotesFunction())
                .name("SecondStepReduceVotes");
        // close transactions
        secondMerged = factory.onClosingSink(secondMerged, this);

        DataStream<TransactionResult> fromSPU = spuResultsPerTGraph.get(tGraphID);
        DataStream<TransactionResult> results = secondMerged
                .flatMap(new CloseFunction(createTransactionalRuntimeContext(tGraphID)))
                .name("CloseFunction")
                .union(fromSPU);

        if (!baselineMode) {
            // filter out replayed values and null elements (generated by filters in the topology)
            results = results
                    .filter(r -> r.f3 != Vote.REPLAY)
                    .name("FilterREPLAYed");
        }

        return results;
    }

    public void setTaskManagers(String[] taskManagers) {
        this.taskManagers = taskManagers;
    }

    public String[] getTaskManagers() {
        return taskManagers;
    }

    public void setCloseBatchSize(int closeBatchSize) {
        this.closeBatchSize = closeBatchSize;
    }

    public int getCloseBatchSize() {
        return closeBatchSize;
    }

    private static class LastStepAdder implements MapFunction<Metadata, Metadata> {
        private final int offset, batchSize;

        public LastStepAdder(int offset, int batchSize) {
            this.offset = offset;
            this.batchSize = batchSize;
        }

        @Override
        public Metadata map(Metadata metadata) throws Exception {
            metadata.batchID.addStepManually(offset, batchSize);
            return metadata;
        }
    }

    private static class MetadataExtractor<T extends Enriched<?>> implements MapFunction<T, Metadata> {
        @Override
        public Metadata map(T enriched) throws Exception {
            return enriched.metadata;
        }
    }

    private static class EmptySPUSource extends ControlledSource<SinglePartitionUpdate> {

        @Override
        public void run(SourceContext<SinglePartitionUpdate> sourceContext) throws Exception {
            // does nothing
            waitForFinish();
        }

        @Override
        public void cancel() {

        }
    }
}
