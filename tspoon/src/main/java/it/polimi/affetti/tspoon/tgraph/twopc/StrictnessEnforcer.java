package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by affo on 03/08/17.
 * <p>
 * Used only at PL4 level. Must be single threaded.
 * <p>
 * Orders transactions and analyzes dependencies among them and enforces replay if a later transaction
 * is succeeding while an earlier and conflicting one is toReplay because of the conflict.
 * <p>
 * Moreover, the function adds the dependency to the later transaction.
 */
public class StrictnessEnforcer extends RichFlatMapFunction<Metadata, Metadata> {
    private transient CompleteBatcher completeBatchChecker;
    private Map<Integer, Metadata> metadataCache;
    private Set<Integer> replaySet;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        completeBatchChecker = new CompleteBatcher();
        metadataCache = new HashMap<>();
        replaySet = new HashSet<>();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<Metadata> collector) throws Exception {
        metadataCache.put(metadata.tid, metadata);

        if (metadata.vote == Vote.REPLAY) {
            completeBatchChecker.signalReplay(metadata.tid);
            collector.collect(metadata);
            return;
        }

        // this is a forward dependency
        if (replaySet.remove(metadata.tid)) {
            metadata.vote = Vote.REPLAY;
            collector.collect(metadata);
            return;
        }

        completeBatchChecker.addTransaction(metadata.tid);

        for (Integer forwardDependency :
                extractForwardDependencies(metadata.dependencyTracking)) {
            completeBatchChecker.signalReplay(forwardDependency);
            replaySet.add(forwardDependency);
        }

        completeBatchChecker.nextBatch().forEach(
                tid -> {
                    Metadata m = metadataCache.remove(tid);
                    if (replaySet.remove(tid)) {
                        m.vote = Vote.REPLAY;
                    }
                    collector.collect(m);
                }
        );
    }

    private Set<Integer> extractForwardDependencies(Set<Integer> dependencies) {
        Set<Integer> forwardDependencies = new HashSet<>();
        Iterator<Integer> iterator = dependencies.iterator();
        while (iterator.hasNext()) {
            Integer next = iterator.next();
            if (next < 0) {
                forwardDependencies.add(-next);
                iterator.remove();
            }
        }

        return forwardDependencies;
    }

    /**
     * Provides complete batches basing on the replays signalled.
     * The idea is that we must gather every transaction until a certain threshold in order to be sure
     * that no transaction i happened-before transaction j specifies a forward dependency towards j.
     * In other words, we cannot emit tnx j before processing i, if i < j.
     */
    public static class CompleteBatcher {
        // cannot trust timestamps, they are not ordered wrt tids!
        private OrderedTimestamps tids = new OrderedTimestamps();
        // tids toReplay
        private Set<Integer> toReplay = new HashSet<>();
        private int lastRemoved = 0;

        public void signalReplay(int tid) {
            toReplay.add(tid);
        }

        /**
         * Every time that a new value comes, it gets added to the internal tids.
         * <p>
         * REPLAY indexing gets updated.
         *
         * @param
         */
        public void addTransaction(int tid) {
            tids.addInOrderWithoutRepetition(tid);
            toReplay.remove(tid);
        }

        /**
         * <p>
         * Once you get a contiguous sequence, you can return every record until the first replay,
         * plus every record in status REPLAY.
         * <p>
         * The algorithm keeps the list of order tids consistent with the fact that REPLAYs happen.
         * For instance, if we have 1 -> 2 -> 3 -> 4 and 3 must be replayed, we can't remove the entire sequence.
         * Otherwise, when 3 will be replayed, it will be alone (no 2 and 4 will come back) and block the processing.
         * <p>
         * NOTE that you cannot collect a record if every one before it either committed or aborted!
         * For example:
         * - T10 updates object X, but X has version 9 and 10 has watermark 8 --> 10 has to be REPLAYed (dependency = [9]);
         * - T11 updates object X, 11 has watermark 9 --> no problem, T11 COMMIT;
         * - T10 gets replayed and now it has a forward dependency with T11, but T11 has already COMMITted!
         *
         * @return
         */
        public List<Integer> nextBatch() {
            int upperThreshold = toReplay.isEmpty() ? Integer.MAX_VALUE : Collections.min(toReplay);
            List<Integer> batch = tids.removeContiguousWith(lastRemoved, upperThreshold);

            for (Integer element : batch) {
                toReplay.remove(element);
                lastRemoved = element;
            }

            return batch;
        }
    }
}
