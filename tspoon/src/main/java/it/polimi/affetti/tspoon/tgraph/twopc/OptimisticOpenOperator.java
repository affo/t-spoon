package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by affo on 14/07/17.
 * <p>
 * Synchronization is offered by the OpenOperator class.
 */
public class OptimisticOpenOperator<T> extends OpenOperator<T> {
    private int watermark = 0;
    private int lastCommittedWatermark = 0;
    // NOTE: everything is indexed by transaction id (tid)
    private Map<Integer, T> elements = new HashMap<>();
    // set of tids depends on tid (tid -> [tids, ...])
    private Map<Integer, Set<Integer>> dependencies = new HashMap<>();
    private Set<Integer> laterReplay = new HashSet<>();

    private Map<Integer, LocalTransactionContext> executions = new HashMap<>();

    private transient TransactionsIndex transactionsIndex;
    private transient WAL wal;

    @Override
    public void open() throws Exception {
        super.open();
        if (TransactionEnvironment.isolationLevel == IsolationLevel.PL4) {
            transactionsIndex = new TidTransactionsIndex();
        } else {
            transactionsIndex = new StandardTransactionsIndex();
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
        metadata.timestamp = transactionsIndex.newTimestamps(metadata.tid);

        LocalTransactionContext localContext = new LocalTransactionContext();
        localContext.timestamp = metadata.timestamp;
        localContext.playedWithWatermark = watermark;

        elements.put(metadata.tid, element.value);
        executions.put(metadata.tid, localContext);
        transactionsIndex.addTransaction(metadata.tid, metadata.timestamp);
    }

    @Override
    protected void onAck(int timestamp, Vote vote, int replayCause, String updates) {
        int tid = transactionsIndex.getTransactionId(timestamp);
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
    // thread-safe
    protected void closeTransaction(int timestamp) {
        int tid = transactionsIndex.getTransactionId(timestamp);
        LocalTransactionContext execution = executions.get(tid);

        Vote vote = execution.vote;
        updateStats(vote);
        Integer dependency = transactionsIndex.getTransactionId(execution.replayCause);

        transactionsIndex.updateWatermark(tid, timestamp, vote);
        int oldWM = watermark;
        Integer lastCompleted = transactionsIndex.getLastCompleted();
        watermark = lastCompleted != null ? lastCompleted : watermark;
        boolean wmUpdate = watermark > oldWM;

        if (wmUpdate) {
            laterReplay.forEach(this::collect);
            laterReplay.clear();
        }

        switch (vote) {
            case COMMIT:
                if (timestamp > lastCommittedWatermark) {
                    lastCommittedWatermark = timestamp;
                }

                if (watermark >= lastCommittedWatermark) {
                    collector.safeCollect(watermarkTag, lastCommittedWatermark);
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

    @Override
    protected void logTransaction(long timestamp, Vote vote) {
        Tuple2<Long, Vote> logEntry = transactionsIndex.getLogEntryFor(timestamp, vote);
        if (logEntry != null) {
            collector.collectInOrder(logTag, logEntry, timestamp);
        }
    }

    private void updateStats(Vote vote) {
        stats.get(vote).add(1);
    }

    private void onTermination(int tid) {
        // cleanup
        elements.remove(tid);
        executions.remove(tid);
        transactionsIndex.deleteTransaction(tid);

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
        int tid = transactionsIndex.getTransactionId(timestamp);
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
