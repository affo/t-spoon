package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedElements;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by affo on 14/07/17.
 */
public class OptimisticOpenOperator<T> extends OpenOperator<T> {
    // I need a timestamp because a transaction can possibly be executed
    // more than once. However, I need to keep it separate from the transaction ID
    // (i.e. not overwrite it), because I want to track the dependencies among
    // transactions. I use the timestamp to check on the state operators if a
    // transaction has to be replayed or not; on the other hand, I use the transaction ID
    // to track the dependencies among transactions (a dependency is specified only if the
    // conflicting transaction has a timestamp greater than the last version saved).
    private AtomicInteger timestamp = new AtomicInteger(0);
    private int watermark = 0;
    // NOTE: everything is indexed by transaction id (tid)
    private Map<Integer, T> elements = new HashMap<>();
    // set of tids depends on tid (tid -> [tids, ...])
    private Map<Integer, Set<Integer>> dependencies = new HashMap<>();
    private Set<Integer> laterReplay = new HashSet<>();

    private Map<Integer, LocalTransactionContext> executions = new ConcurrentHashMap<>();
    // timestamp -> tid
    private Map<Integer, Integer> timestampTidMapping = new ConcurrentHashMap<>();

    private transient OrderedElements<Integer> watermarks;
    private transient WAL wal;

    @Override
    public void open() throws Exception {
        super.open();
        watermarks = new OrderedElements<>(Comparator.comparingInt(i -> i));

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
        int timestamp = this.timestamp.incrementAndGet();

        Metadata metadata = element.metadata;

        metadata.coordinator = myAddress;
        metadata.timestamp = timestamp;
        metadata.watermark = watermark;

        LocalTransactionContext localContext = new LocalTransactionContext();
        localContext.timestamp = timestamp;
        localContext.playedWithWatermark = watermark;

        elements.put(element.metadata.tid, element.value);
        executions.put(element.metadata.tid, localContext);
        timestampTidMapping.put(timestamp, element.metadata.tid);
    }

    @Override
    protected void onAck(int timestamp, Vote vote, int replayCause, String updates) {
        int tid = timestampTidMapping.get(timestamp);
        LocalTransactionContext execution = executions.get(tid);
        execution.replayCause = replayCause;
        execution.mergeUpdates(updates);
        execution.vote = vote;
    }

    private Integer getLastWM() {
        Integer polled;
        int previous = watermark;

        do {
            polled = watermarks.isEmpty() ? null :
                    watermarks.pollFirstConditionally(watermark + 1, 0);
            if (polled != null) {
                previous = polled;
            }
        } while (polled != null);

        return previous;
    }

    private void replayElement(Integer tid) {
        // do not replay with the same watermark...
        if (executions.get(tid).playedWithWatermark < watermark) {
            collect(tid);
        } else {
            laterReplay.add(tid);
        }
    }

    private void addDependency(int tid, int dependency) {
        dependencies.computeIfAbsent(dependency, k -> new HashSet<>()).add(tid);
    }

    @Override
    protected void closeTransaction(int timestamp) {
        int tid = timestampTidMapping.remove(timestamp);
        LocalTransactionContext execution = executions.get(tid);

        Vote vote = execution.vote;
        Integer dependency = timestampTidMapping.get(execution.replayCause);

        watermarks.addInOrder(timestamp);
        int oldWM = watermark;
        watermark = getLastWM();
        boolean wmUpdate = watermark > oldWM;

        if (wmUpdate) {
            laterReplay.forEach(this::collect);
            laterReplay.clear();
        }

        switch (vote) {
            case COMMIT:
            case ABORT:
                if (wmUpdate) {
                    // TODO this does not mean that the watermark is for a committed transaction!
                    collector.safeCollect(watermark);
                }

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

    private void onTermination(int tid) {
        elements.remove(tid);
        executions.remove(tid);

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
