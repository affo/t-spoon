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
public class Transactions {
    private TwoPCFactory factory;
    public static IsolationLevel isolationLevel = PL3; // max level by default

    public Transactions(Strategy strategy, IsolationLevel isolationLevel) {
        Transactions.isolationLevel = isolationLevel;

        switch (strategy) {
            case PESSIMISTIC:
                // TODO implement
                // For now, it falls into optimistic
            default:
                this.factory = new OptimisticTwoPCFactory();
        }
    }

    public <T> TStream<T> open(DataStream<T> ds) {
        return factory.open(ds);
    }

    public <T> List<DataStream<T>> close(TStream<T>... exitPoints) {
        int n = exitPoints.length;
        assert n >= 1;

        List<DataStream<Enriched<T>>> encloseds = new ArrayList<>(n);
        List<DataStream<TwoPCData>> firstStepMerged = new ArrayList<>(n);
        for (TStream<T> exitPoint : exitPoints) {
            DataStream<Enriched<T>> enclosed = exitPoint.getEnclosingStream();
            encloseds.add(enclosed);
            // first step reduction of votes on each exit point
            DataStream<TwoPCData> twoPC = enclosed.map(
                    new MapFunction<Enriched<T>, TwoPCData>() {
                        @Override
                        public TwoPCData map(Enriched<T> e) throws Exception {
                            return e.tContext().twoPC;
                        }
                    }
            );
            DataStream<TwoPCData> reduced = twoPC.keyBy(tpc -> tpc.tid).flatMap(new ReduceVotesFunction());
            firstStepMerged.add(reduced);
        }

        // second step reduction on every exit point using a batch size
        // equal to the number of exit points
        MapFunction<TwoPCData, TwoPCData> assignBatchSize = meta -> {
            meta.batchSize = n;
            return meta;
        };
        DataStream<TwoPCData> union = firstStepMerged.get(0).map(assignBatchSize);
        for (DataStream<TwoPCData> unite : firstStepMerged.subList(1, n)) {
            union = union.union(unite.map(assignBatchSize));
        }

        DataStream<TwoPCData> secondMerged = union.keyBy(m -> m.tid).flatMap(new ReduceVotesFunction());
        // close transactions
        factory.close(secondMerged);

        // output valid records and unwrap
        List<DataStream<T>> result = new ArrayList<>(n);
        for (DataStream<Enriched<T>> enclosed : encloseds) {
            DataStream<Enriched<T>> valid = enclosed
                    .connect(secondMerged)
                    .keyBy(new KeySelector<Enriched<T>, Integer>() {
                               @Override
                               public Integer getKey(Enriched<T> e) throws Exception {
                                   return e.tContext().getTid();
                               }
                           },
                            new KeySelector<TwoPCData, Integer>() {
                                @Override
                                public Integer getKey(TwoPCData m) throws Exception {
                                    return m.tid;
                                }
                            })
                    .flatMap(new BufferRecordFunction<>());
            DataStream<T> unwrapped = valid.map(
                    new MapFunction<Enriched<T>, T>() {
                        @Override
                        public T map(Enriched<T> tEnriched) throws Exception {
                            return tEnriched.value();
                        }
                    });
            result.add(unwrapped);
        }

        return result;
    }
}
