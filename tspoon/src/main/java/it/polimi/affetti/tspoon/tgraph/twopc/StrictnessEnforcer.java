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
        private Set<Integer> collected = new HashSet<>();
        private int lastRemoved = 0;

        public void addMetadata(Metadata metadata) {
            tids.addInOrder(metadata.tid);
            metas.put(metadata.tid, metadata);
        }

        public List<Metadata> next() {
            List<Metadata> result = new LinkedList<>();

            // consider the last removed as part of the tids
            // to enforce contiguity with the last batch removed
            tids.addInOrder(lastRemoved);
            List<Integer> batch = tids.getContiguousElements();
            tids.remove(lastRemoved);
            batch.remove(0);

            if (batch.isEmpty()) {
                return Collections.emptyList();
            }

            int offset = batch.get(0);
            // the firstReplay is outside of the batch
            int firstReplay = offset + batch.size();

            for (Integer tid : batch) {
                if (!collected.contains(tid)) {
                    Metadata metadata = metas.get(tid);
                    result.add(metadata);

                    if (metadata.vote == Vote.REPLAY) {
                        firstReplay = Math.min(firstReplay, tid);
                    }

                    for (Integer aboveWatermark : metadata.dependencyTracking) {
                        Metadata otherMetadata = metas.get(aboveWatermark);

                        if (otherMetadata == null) {
                            // don't know nothing about this transaction, we need to wait...
                            return Collections.emptyList();
                        }

                        if (aboveWatermark > tid) {
                            // this means that the current transaction could see the version
                            // of a later transaction (real transaction execution does not respect timestamps) --- or,
                            // the other way around, the later transaction acted as if the current one never happened.
                            // This implies that the later transaction should be replayed!
                            otherMetadata.vote = Vote.REPLAY;

                            firstReplay = Math.min(firstReplay, aboveWatermark);
                        }
                    }

                    // transform the dependencies of metadata in timestamps for proper use in OpenOperator
                    metadata.dependencyTracking = metadata.dependencyTracking.stream().map(
                            dependencyTid -> metas.get(dependencyTid).timestamp).collect(Collectors.toSet());
                }
            }

            List<Integer> removed = tids.removeContiguous(firstReplay);
            for (Integer r : removed) {
                collected.remove(r);
                lastRemoved = r;
            }

            for (Metadata toCollect : result) {
                if (toCollect.vote != Vote.REPLAY) {
                    collected.add(toCollect.tid);
                } else {
                    // remove from metadata because it will come back
                    tids.remove(toCollect.tid);
                }
            }

            return result;
        }
    }
}
