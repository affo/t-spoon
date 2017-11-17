package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.query.*;
import it.polimi.affetti.tspoon.tgraph.twopc.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

import static it.polimi.affetti.tspoon.tgraph.IsolationLevel.PL3;
import static it.polimi.affetti.tspoon.tgraph.IsolationLevel.PL4;

/**
 * Created by affo on 17/07/17.
 * <p>
 * Represents the transactional graph context
 */
public class TransactionEnvironment {
    private static TransactionEnvironment instance;
    private boolean isDurabilityEnabled = true;
    private QuerySource querySource;
    private DataStream<MultiStateQuery> queryStream;
    private TwoPCFactory factory;
    private IsolationLevel isolationLevel = PL3; // max level by default
    private boolean useDependencyTracking = true;
    private boolean verbose = false;
    private long deadlockTimeout;

    private TransactionEnvironment(StreamExecutionEnvironment env) {
        this.querySource = new QuerySource();
        this.queryStream = env.addSource(querySource).name("QuerySource");
    }

    public synchronized static TransactionEnvironment get() {
        if (instance == null) {
            instance = new TransactionEnvironment(StreamExecutionEnvironment.getExecutionEnvironment());
        }
        return instance;
    }

    // only to run 2 jobs
    public synchronized static void clear() {
        instance = null;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setStrategy(Strategy strategy) {
        switch (strategy) {
            case PESSIMISTIC:
                this.factory = new PessimisticTwoPCFactory();
                break;
            default:
                this.factory = new OptimisticTwoPCFactory();
        }
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;

        if (isolationLevel == PL4) {
            useDependencyTracking = true;
        }
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public void setUseDependencyTracking(boolean useDependencyTracking) {
        if (isolationLevel == PL4) {
            // cannot change dependency tracking policy at level PL4
            return;
        }

        this.useDependencyTracking = useDependencyTracking;
    }

    public boolean usingDependencyTracking() {
        return useDependencyTracking;
    }

    public void setDurable(boolean durable) {
        isDurabilityEnabled = durable;
    }

    public boolean isDurabilityEnabled() {
        return isDurabilityEnabled;
    }

    public void setDeadlockTimeout(long deadlockTimeout) {
        this.deadlockTimeout = deadlockTimeout;
    }

    public long getDeadlockTimeout() {
        return deadlockTimeout;
    }

    public void setQuerySupplier(QuerySupplier querySupplier) {
        querySource.setQuerySupplier(querySupplier);
    }

    public <T> OpenStream<T> open(DataStream<T> ds) {
        return open(ds, null);
    }

    public <T> OpenStream<T> open(DataStream<T> ds, QuerySender.OnQueryResult onQueryResult) {
        // TODO every tGraph should receive queries only for the states it is responsible for!
        // In this implementation every tGraph receives every query...
        // NOTE: for the Evaluation it is ok, because we only query on a single tGraph (on a single state)
        OpenStream<T> openStream = factory.open(ds);
        QuerySender querySender;
        if (onQueryResult == null) {
            querySender = new QuerySender();
        } else {
            querySender = new QuerySender(onQueryResult);
        }

        // TODO differentiate querying part for pessimistic case!
        // TODO Up to now it is still not implemented...
        if (openStream.watermarks == null) {
            return openStream;
        }

        openStream.watermarks.connect(queryStream).flatMap(new QueryProcessor()).name("QueryProcessor")
                // TODO it should be:
                //      processed = queryStream.flatMap(new QueryProcessor()).select(... byStateName ...)
                // outside of this function, in TransactionEnvironment.get()
                // and later:
                //      processed.select(... the stateNames of this tGraph ...).connect(watermarks).addSink(querySender)
                .addSink(querySender).name("QuerySender");
        return openStream;
    }

    public <T> List<DataStream<TransactionResult<T>>> close(TStream<T>... exitPoints) {
        int n = exitPoints.length;
        assert n >= 1;

        List<DataStream<Enriched<T>>> encloseds = new ArrayList<>(n);
        List<DataStream<Metadata>> firstStepMerged = new ArrayList<>(n);
        for (TStream<T> exitPoint : exitPoints) {
            DataStream<Enriched<T>> enclosed = exitPoint.getEnclosingStream();
            encloseds.add(enclosed);
            // first step reduction of votes on each exit point
            DataStream<Metadata> twoPC = enclosed.map(
                    new MapFunction<Enriched<T>, Metadata>() {
                        @Override
                        public Metadata map(Enriched<T> e) throws Exception {
                            return e.metadata;
                        }
                    }
            );
            DataStream<Metadata> reduced = twoPC
                    .keyBy(tpc -> tpc.timestamp)
                    .flatMap(new ReduceVotesFunction())
                    .name("FirstStepReduceVotes");
            firstStepMerged.add(reduced);
        }

        // second step reduction on every exit point using a batch size
        // equal to the number of exit points
        MapFunction<Metadata, Metadata> assignBatchSize = meta -> {
            meta.batchSize = n;
            return meta;
        };
        DataStream<Metadata> union = firstStepMerged.get(0).map(assignBatchSize);
        for (DataStream<Metadata> unite : firstStepMerged.subList(1, n)) {
            union = union.union(unite.map(assignBatchSize));
        }

        DataStream<Metadata> secondMerged = union
                .keyBy(m -> m.timestamp)
                .flatMap(new ReduceVotesFunction())
                .name("SecondStepReduceVotes");
        // close transactions
        secondMerged = factory.onClosingSink(secondMerged);
        secondMerged.addSink(new CloseSink(factory.getSinkTransactionCloser())).name("CloseSink");

        // output valid records and unwrap
        List<DataStream<TransactionResult<T>>> result = new ArrayList<>(n);
        for (DataStream<Enriched<T>> enclosed : encloseds) {
            DataStream<Enriched<T>> valid = enclosed
                    .connect(secondMerged)
                    .keyBy(
                            new KeySelector<Enriched<T>, Integer>() {
                                @Override
                                public Integer getKey(Enriched<T> e) throws Exception {
                                    return e.metadata.timestamp;
                                }
                            },
                            new KeySelector<Metadata, Integer>() {
                                @Override
                                public Integer getKey(Metadata m) throws Exception {
                                    return m.timestamp;
                                }
                            })
                    .flatMap(new BufferFunction<>()).name("Buffer");
            DataStream<TransactionResult<T>> unwrapped = valid
                    .filter(enriched -> enriched.metadata.vote != Vote.REPLAY)
                    .map(
                            new MapFunction<Enriched<T>, TransactionResult<T>>() {
                                @Override
                                public TransactionResult<T> map(Enriched<T> enriched) throws Exception {
                                    Metadata metadata = enriched.metadata;
                                    return new TransactionResult<>(metadata.tid, metadata.vote, enriched.value);
                                }
                            });
            result.add(unwrapped);
        }

        return result;
    }
}
