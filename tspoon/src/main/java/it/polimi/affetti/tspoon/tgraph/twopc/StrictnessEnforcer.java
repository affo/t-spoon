package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by affo on 03/08/17.
 * <p>
 * Used only at PL4 level. Must be single threaded.
 * <p>
 * Orders transactions and analyzes dependencies among them and enforces replay if a later transaction
 * is succeeding while an earlier and conflicting one is replaying because of the conflict.
 * <p>
 * Moreover, the function adds the dependency to the later transaction.
 */
public class StrictnessEnforcer extends RichFlatMapFunction<Metadata, Metadata> {
    private transient DependencyTracker dependencyTracker;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dependencyTracker = new DependencyTracker();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<Metadata> collector) throws Exception {
        dependencyTracker.addMetadata(metadata);

        for (Metadata m : dependencyTracker.next()) {
            collector.collect(m);
        }
    }

    public static class DependencyTracker {
        private OrderedTimestamps tids = new OrderedTimestamps();
        private Map<Integer, Metadata> metas = new HashMap<>();
        private Set<Integer> replaying = new HashSet<>();
        // saves the tids of records from forward dependencies
        // which we still don't have the metadata for: tid -> forwardDependencyCause
        private Map<Integer, Integer> toReplay = new HashMap<>();
        private int lastRemoved = 0;
        private int lastReplay = Integer.MAX_VALUE;

        private Metadata moveToReplaying(Metadata metadata) {
            metadata.vote = Vote.REPLAY;
            Integer forwardDependencyCause = toReplay.remove(metadata.tid);
            replaying.add(metadata.tid);

            // cut out forward dependencies for proper use in OpenOperator
            metadata.dependencyTracking.removeIf(tid -> tid >= metadata.tid);
            // add the forward dependency cause
            metadata.dependencyTracking.add(forwardDependencyCause);

            return metadata;
        }

        /**
         * Every time that a new value comes, it gets added to the internal tids.
         * <p>
         * REPLAY indexing gets updated.
         *
         * @param metadata
         */
        public void addMetadata(Metadata metadata) {
            metas.put(metadata.tid, metadata);
            tids.addInOrderWithoutRepetition(metadata.tid);

            if (toReplay.containsKey(metadata.tid)) {
                // forward dependency I have never seen
                // let's save it as a future replay (it will come back)
                moveToReplaying(metadata);
            } else if (replaying.contains(metadata.tid) && metadata.vote != Vote.REPLAY) {
                // not a forward dependency, but vote != REPLAY,
                // it is a record registered as a replay that now succesfully committed/aborted
                // it can be removed because it will never come back.
                replaying.remove(metadata.tid);
            }

            if (metadata.vote == Vote.REPLAY) {
                // I have never saw this record before and it is REPLAY,
                // save it for later
                replaying.add(metadata.tid);
            }

            for (Integer aboveWatermark : metadata.dependencyTracking) {
                if (aboveWatermark > metadata.tid) {
                    // this means that the current transaction could see the version
                    // of a later transaction (real transaction execution does not respect timestamps) --- or,
                    // the other way around, the later transaction acted as if the current one never happened.
                    // This implies that the later transaction should be replayed!
                    toReplay.put(aboveWatermark, metadata.tid);

                    // update consistently metadata's vote if present
                    metas.computeIfPresent(aboveWatermark, (tid, meta) -> moveToReplaying(meta));
                }
            }

            // update last replay
            // I don't worry about toReplay, because I know they are not here at the moment
            lastReplay = replaying.isEmpty() ? Integer.MAX_VALUE : Collections.min(replaying);
        }

        /**
         * The idea is that you cannot allow a value to be returned if you haven't processed every record before it.
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
        public List<Metadata> next() {
            List<Integer> toCollect = tids.removeContiguousWith(lastRemoved, lastReplay);

            // we update the lastRemoved only in case we detected a new contiguous batch
            if (!toCollect.isEmpty()) {
                lastRemoved = Collections.max(toCollect);
            }

            // collect any record that needs REPLAY
            toCollect.addAll(replaying);

            return toCollect.stream().map(metas::remove).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }
}
