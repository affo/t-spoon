package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by affo on 14/07/17.
 * <p>
 * Synchronization is offered by the OpenOperator class.
 */
public class OptimisticOpenOperator<T> extends OpenOperator<T> {
    // I need a timestamp because a transaction can possibly be executed
    // more than once. However, I need to keep it separate from the transaction ID
    // (i.e. not overwrite it), because I want to track the dependencies among
    // transactions. I use the timestamp to check on the state operators if a
    // transaction has to be replayed or not; on the other hand, I use the transaction ID
    // to track the dependencies among transactions (a dependency is specified only if the
    // conflicting transaction has a timestamp greater than the last version saved).
    private AtomicInteger currentTimestamp = new AtomicInteger(0);
    private int watermark = 0;
    private int lastCommittedWatermak = 0;
    // NOTE: everything is indexed by transaction id (tid)
    private Map<Integer, T> elements = new HashMap<>();
    // set of tids depends on tid (tid -> [tids, ...])
    private Map<Integer, Set<Integer>> dependencies = new HashMap<>();
    private Set<Integer> laterReplay = new HashSet<>();

    private Map<Integer, LocalTransactionContext> executions = new HashMap<>();
    // timestamp -> tid
    private Map<Integer, Integer> timestampTidMapping = new HashMap<>();
    private Map<Integer, Set<Integer>> tidTimestampMapping = new HashMap<>();

    private transient WatermarkingStrategy watermarkingStrategy;
    private transient WAL wal;

    @Override
    public void open() throws Exception {
        super.open();
        if (TransactionEnvironment.isolationLevel == IsolationLevel.PL4) {
            watermarkingStrategy = new TidWatermarkingStrategy();
        } else {
            watermarkingStrategy = new StandardWatermarkingStrategy();
        }

        // TODO send to kafka
        // up to now, we only introduce overhead by writing to disk
        wal = new DummyWAL("wal.log");
        wal.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        wal.close();
    }

    private void collect(int tid) {
        Metadata metadata = new Metadata(tid);
        metadata.coordinator = myAddress;
        Enriched<T> out = Enriched.of(metadata, elements.get(tid));
        openTransaction(out);
        collector.safeCollect(out);
    }

    @Override
    protected void openTransaction(Enriched<T> element) {
        Metadata metadata = element.metadata;

        metadata.coordinator = myAddress;
        metadata.watermark = watermark;
        metadata.timestamp = currentTimestamp.incrementAndGet();

        LocalTransactionContext localContext = new LocalTransactionContext();
        localContext.timestamp = metadata.timestamp;
        localContext.playedWithWatermark = watermark;

        elements.put(metadata.tid, element.value);
        executions.put(metadata.tid, localContext);
        timestampTidMapping.put(metadata.timestamp, metadata.tid);
        tidTimestampMapping.computeIfAbsent(metadata.tid, k -> new HashSet<>()).add(metadata.timestamp);
    }

    @Override
    protected void onAck(int timestamp, Vote vote, int replayCause, String updates) {
        int tid = timestampTidMapping.get(timestamp);
        LocalTransactionContext execution = executions.get(tid);
        execution.replayCause = replayCause;
        execution.mergeUpdates(updates);
        execution.vote = vote;
    }

    private void replayElement(Integer tid) {
        // do not replay with the same watermark...
        if (executions.get(tid).playedWithWatermark < watermark) {
            collect(tid);
        } else {
            laterReplay.add(tid);
        }
    }

    private void addDependency(int dependsOn, int tid) {
        dependencies.computeIfAbsent(tid, k -> new HashSet<>()).add(dependsOn);
    }

    @Override
    protected void closeTransaction(int timestamp) {
        int tid = timestampTidMapping.get(timestamp);
        LocalTransactionContext execution = executions.get(tid);

        Vote vote = execution.vote;
        updateStats(vote);
        Integer dependency = timestampTidMapping.get(execution.replayCause);

        watermarkingStrategy.notifyTermination(tid, timestamp, vote);
        int oldWM = watermark;
        Integer lastCompleted = watermarkingStrategy.getLastCompleted();
        watermark = lastCompleted != null ? lastCompleted : watermark;
        boolean wmUpdate = watermark > oldWM;

        if (wmUpdate) {
            laterReplay.forEach(this::collect);
            laterReplay.clear();
        }

        switch (vote) {
            case COMMIT:
                if (timestamp > lastCommittedWatermak) {
                    lastCommittedWatermak = timestamp;
                }

                if (watermark > lastCommittedWatermak) {
                    collector.safeCollect(lastCommittedWatermak);
                }
            case ABORT:
                onTermination(tid);
                break;
            case REPLAY:
                if (dependency != null && tid > dependency && elements.get(dependency) != null) {
                    // the timestamp has a mapping in tids
                    // this transaction depends on a previous one
                    // the transaction on which the current one depends has not commited/aborted
                    addDependency(tid, dependency);
                } else {
                    replayElement(tid);
                }
        }
    }

    private void updateStats(Vote vote) {
        stats.get(vote).add(1);
    }

    private void onTermination(int tid) {
        // cleanup
        elements.remove(tid);
        executions.remove(tid);
        Set<Integer> timestamps = tidTimestampMapping.remove(tid);
        timestamps.forEach(ts -> timestampTidMapping.remove(ts));

        Set<Integer> deps = dependencies.remove(tid);
        if (deps != null) {
            // draw a chain of ordered dependencies
            ArrayList<Integer> depList = new ArrayList<>(deps);
            Collections.sort(depList);
            for (int i = 0; i < depList.size() - 1; i++) {
                addDependency(depList.get(i + 1), depList.get(i));
            }

            // replay only the first one
            replayElement(depList.get(0));
        }
    }

    private class LocalTransactionContext {
        public int timestamp;
        public int playedWithWatermark;
        public Vote vote;
        public int replayCause;
        public String updates = "";

        public void mergeUpdates(String updates) {
            this.updates += updates;
        }
    }

    // durability
    @Override
    protected void writeToWAL(int timestamp) throws IOException {
        int tid = timestampTidMapping.get(timestamp);
        switch (executions.get(tid).vote) {
            case REPLAY:
                wal.replay(timestamp);
                break;
            case ABORT:
                wal.abort(timestamp);
                break;
            default:
                wal.commit(timestamp, executions.get(tid).updates);
        }
    }
}
