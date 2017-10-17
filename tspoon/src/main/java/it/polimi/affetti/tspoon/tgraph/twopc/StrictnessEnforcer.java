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
        //private Set<Integer> collected = new HashSet<>();
        private Set<Integer> toReplay = new HashSet<>();
        private int lastRemoved = 0;

        /**
         * Every time that a new value comes, it is added to the internal tids.
         *
         * @param metadata
         */
        public void addMetadata(Metadata metadata) {
            metas.put(metadata.tid, metadata);
            tids.addInOrder(metadata.tid);

            if (metadata.vote == Vote.REPLAY) {
                toReplay.add(metadata.tid);
            }

            for (Integer aboveWatermark : metadata.dependencyTracking) {
                if (aboveWatermark > metadata.tid) {
                    // this means that the current transaction could see the version
                    // of a later transaction (real transaction execution does not respect timestamps) --- or,
                    // the other way around, the later transaction acted as if the current one never happened.
                    // This implies that the later transaction should be replayed!
                    toReplay.add(aboveWatermark);
                }
            }

        }

        // cut out forward dependencies for proper use in OpenOperator
        private void updateDependencies(Metadata metadata) {
            metadata.dependencyTracking.removeIf(tid -> tid >= metadata.tid);
        }

        /**
         * The idea is that you cannot allow a value to be returned if you haven't processed every record before it.
         * <p>
         * Once you get a contiguous sequence, you can return it entirely, plus the records to be replayed.
         * The algorithm keeps the list of order tids consistent with the fact that REPLAY happends.
         * For instance, if we have 1 -> 2 -> 3 -> 4 and 3 must be replayed, we can't remove the entire sequence.
         * Otherwise, when 3 will be replayed, it will be alone (no 2 and 4 will come back) and block the processing.
         *
         * @return
         */
        public List<Metadata> next() {
            // every record contiguous with the last remove can be sent out
            // the problem is to keep it in the ordered timestamp not to create holes
            Set<Integer> toCollect = new HashSet<>(tids.getContiguousWith(lastRemoved));

            // we update the lastRemoved only in case we detected a new contiguous batch
            if (!toCollect.isEmpty()) {
                // set the remove threshold to the minimum of the replays
                int minReplay = toReplay.isEmpty() ? Integer.MAX_VALUE : Collections.min(toReplay);
                int maxCollect = Collections.max(toCollect);
                int removeThreshold = Math.min(minReplay, maxCollect + 1);

                // remove until first replay (avoid holes for when replays will come)
                tids.removeContiguousWith(lastRemoved, removeThreshold);
                lastRemoved = removeThreshold - 1;
            }

            List<Metadata> result = new LinkedList<>();

            toCollect.addAll(toReplay);

            for (Integer tid : toCollect) {
                Metadata metadata = metas.remove(tid);

                // it could be a tuple that we have never seen before,
                // or it could be something that had already been collected and not replayed
                // so, check for null!
                if (metadata != null) {
                    if (toReplay.contains(tid)) {
                        metadata.vote = Vote.REPLAY;
                        toReplay.remove(tid);
                    }

                    if (metadata.vote == Vote.REPLAY) {
                        // remove from ids because it will come back
                        tids.remove(tid);
                    }

                    updateDependencies(metadata);
                    result.add(metadata);
                }
            }

            return result;
        }
    }
}
