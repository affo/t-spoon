package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.twopc.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;

import static it.polimi.affetti.tspoon.tgraph.IsolationLevel.PL3;

/**
 * Created by affo on 17/07/17.
 * <p>
 * Represents the transactional graph context
 */
public class TransactionEnvironment {
    private static TransactionEnvironment instance;
    private TwoPCFactory factory;
    public static IsolationLevel isolationLevel = PL3; // max level by default
    public static boolean useDependencyTracking;

    private TransactionEnvironment() {
    }

    public synchronized static TransactionEnvironment get() {
        if (instance == null) {
            instance = new TransactionEnvironment();
        }
        return instance;
    }

    public void setStrategy(Strategy strategy) {
        switch (strategy) {
            case PESSIMISTIC:
                // TODO implement
                // For now, it falls into optimistic
            default:
                this.factory = new OptimisticTwoPCFactory();
        }
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        TransactionEnvironment.isolationLevel = isolationLevel;
    }

    public void setUseDependencyTracking(boolean useDependencyTracking) {
        TransactionEnvironment.useDependencyTracking = useDependencyTracking;
    }

    public <T> OpenStream<T> open(DataStream<T> ds) {
        return factory.open(ds);
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
            DataStream<Metadata> reduced = twoPC.keyBy(tpc -> tpc.timestamp).flatMap(new ReduceVotesFunction());
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

        DataStream<Metadata> secondMerged = union.keyBy(m -> m.timestamp).flatMap(new ReduceVotesFunction());
        // close transactions
        secondMerged.addSink(new CloseSink());

        // output valid records and unwrap
        List<DataStream<TransactionResult<T>>> result = new ArrayList<>(n);
        for (DataStream<Enriched<T>> enclosed : encloseds) {
            DataStream<Enriched<T>> valid = enclosed
                    .connect(secondMerged)
                    .keyBy(new KeySelector<Enriched<T>, Integer>() {
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
                    .flatMap(new BufferRecordFunction<>());
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
