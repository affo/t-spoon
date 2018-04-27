package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by affo on 03/08/17.
 * <p>
 * Used only at PL4 level. Must be single threaded.
 * <p>
 * Orders transactions and analyzes dependencies among them and enforces replay if a later transaction
 * is succeeding while an earlier and conflicting one is toReplay because of the conflict.
 * Moreover, the function adds the dependency to the later transaction.
 * <p>
 * The logic is that if a COMMITted/ABORTed transaction is collected, then every single transaction before (in tid order)
 * has either committed or aborted. The strictness enforcer waits for the replays to happen, in order to validate every
 * subsequent transaction.
 */
public class StrictnessEnforcer extends RichFlatMapFunction<Metadata, Metadata> {
    private transient Sequencer sequencer;
    private Map<Long, Metadata> metadataCache;
    // tid -> newest tid of the transaction that signalled the fwd dependency
    private Map<Long, Long> forwardDependencies;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sequencer = new Sequencer();
        metadataCache = new HashMap<>();
        forwardDependencies = new HashMap<>();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<Metadata> collector) throws Exception {
        long tid = metadata.tid;

        if (metadata.vote == Vote.REPLAY) {
            sequencer.signalReplay(tid);
            collector.collect(metadata);
            return;
        }

        metadataCache.put(tid, metadata);

        // process only when not REPLAY or forward dependency
        if (!forwardDependencies.containsKey(tid)) {
            sequencer.addTransaction(tid);
            // update dependencies and signal forward dependencies
            metadata.dependencyTracking = metadata.dependencyTracking.stream()
                    .flatMap(dep -> {
                        if (dep < 0) {
                            long forwardDep = -dep;

                            sequencer.signalReplay(forwardDep);

                            Long signallerTid = forwardDependencies.get(forwardDep);
                            if (signallerTid == null || signallerTid < tid) {
                                // newer signaller detected
                                forwardDependencies.put(forwardDep, tid);
                            }

                            return Stream.empty();
                        }

                        return Stream.of(dep);
                    })
                    .collect(Collectors.toCollection(HashSet::new));
        }

        // it could be that:
        //  - the Metadata for a previously signalled fwdDep has come -> collect it
        //  - this new record signalled some previously processed Metadata -> collect them (it)
        collectForwardDependencies(collector);

        for (Long toCollect : sequencer.nextAvailableSequence()) {
            Metadata m = metadataCache.remove(toCollect);
            collector.collect(m);
        }
    }

    /**
     * Collects forward dependencies and removes them if collected
     * @param collector
     */
    private void collectForwardDependencies(Collector<Metadata> collector) {
        Iterator<Map.Entry<Long, Long>> iterator = forwardDependencies.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Long> entry = iterator.next();
            long tid = entry.getKey();
            long signallerTid = entry.getValue();
            Metadata metadata = metadataCache.remove(tid);

            // it could be that the Metadata for forward dependency has not yet come
            if (metadata != null) {
                metadata.vote = Vote.REPLAY;
                // add the newest signaller as unique dependency
                metadata.dependencyTracking = new HashSet<>();
                metadata.dependencyTracking.add(signallerTid);

                collector.collect(metadata);
                iterator.remove();
            }
        }
    }

    /**
     * Provides complete batches basing on the replays signalled.
     * The idea is that we must gather every transaction until a certain threshold in order to be sure
     * that no transaction i happened-before transaction j specifies a forward dependency towards j.
     * In other words, we cannot emit tnx j before processing i, if i < j.
     */
    public static class Sequencer {
        // cannot trust timestamps, they are not ordered wrt tids!
        private OrderedTimestamps tids = new OrderedTimestamps();
        private Set<Long> toReplay = new HashSet<>();
        private long lastRemoved = 0;

        public void signalReplay(long tid) {
            toReplay.add(tid);
        }

        /**
         * Every time that a new value comes, it gets added to the internal tids.
         * <p>
         * REPLAY indexing gets updated.
         *
         * @param
         */
        public void addTransaction(long tid) {
            tids.addInOrderWithoutRepetition(tid); // no matter of the real
            toReplay.remove(tid);
        }

        /**
         * <p>
         * Once you get a contiguous sequence, you can return every record until the first replay
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
         * <p>
         * NOTE that the Sequencer provides forward dependencies only when it is useful to collect them.
         * And so when every record before has committed or aborted. The Sequencer, however, does not remove the tids
         * of forward dependencies, because they will be REPLAYed.
         */
        public List<Long> nextAvailableSequence() {
            long firstReplay = toReplay.isEmpty() ? Integer.MAX_VALUE : Collections.min(toReplay);
            List<Long> batch = tids.removeContiguousWith(lastRemoved, firstReplay);

            if (!batch.isEmpty()) {
                lastRemoved = Collections.max(batch);
            }

            return batch;
        }
    }
}

