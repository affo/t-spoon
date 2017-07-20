package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.TransactionContext;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.query.QueryTuple;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by affo on 14/07/17.
 */
public class StateOperator<T, V>
        extends AbstractStreamOperator<Enriched<T>>
        implements TwoInputStreamOperator<Enriched<T>, QueryTuple, Enriched<T>> {
    public final OutputTag<Update<V>> updatesTag;
    // I suppose that the type for keys is String. This assumption is coherent,
    // for instance, with Redis implementation: https://redis.io/topics/data-types-intro
    protected transient Map<String, Object<V>> state;
    // the same transaction could execute more than once on the same stateOperator
    private transient Map<Integer, List<TransactionExecution<T, V>>> executions;
    protected StateFunction<T, V> stateFunction;

    private transient WithServer srv;

    public StateOperator(StateFunction<T, V> stateFunction, OutputTag<Update<V>> updatesTag) {
        this.stateFunction = stateFunction;
        this.updatesTag = updatesTag;
    }

    @Override
    public void open() throws Exception {
        super.open();
        // custom key-level concurrency control
        state = new HashMap<>();
        // protect from concurrent removals on transaction termination
        executions = new ConcurrentHashMap<>();

        srv = new WithServer(new TransactionCloseServer());
        srv.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        srv.close();
    }

    protected synchronized Object<V> getObject(String key) {
        state.putIfAbsent(key, new Object<>());
        return state.get(key);
    }

    @Override
    public void processElement1(StreamRecord<Enriched<T>> sr) throws Exception {
        final String key = getCurrentKey().toString();
        Object<V> versions = getObject(key);

        synchronized (versions) {
            T element = sr.getValue().value();
            TransactionContext tctxt = sr.getValue().tContext();
            tctxt.twoPC.addCohort(srv.getMyAddress());

            TransactionExecution<T, V> tCtxt = tctxt
                    .getTransactionExecution(key, stateFunction, new SafeCollector<>(output, updatesTag, sr));
            executions.putIfAbsent(tctxt.getTid(), new LinkedList<>());
            executions.get(tctxt.getTid()).add(0, tCtxt);

            tCtxt.execute(versions, element);
        }
    }

    /*
    private List<Update<V>> getUpdates(int tid) {
        return state.entrySet().stream()
                .map(entry -> Tuple2.of(entry.getKey(), entry.getValue().getLastVersionBefore(tid)))
                .map(t -> Update.of(tid, t.f0, t.f1.object))
                .collect(Collectors.toList());
    }
    */

    @Override
    public void processElement2(StreamRecord<QueryTuple> sr) throws Exception {
        // TODO implement querying... getUpdates
    }

    /**
     * Stream operators with state, which want to participate in a snapshot need to override this hook method.
     *
     * @param context context that provides information and means required for taking a snapshot
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    /**
     * Stream operators with state which can be restored need to override this hook method.
     *
     * @param context context that allows to register different states.
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
    }

    private class TransactionCloseServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            String[] tokens = request.split(",");
            int tid = Integer.parseInt(tokens[0]);
            Vote vote = Vote.values()[Integer.parseInt(tokens[1])];

            // concurrent removals
            TransactionExecution<T, V> execution = executions.get(tid).remove(0);
            Object<V> versions = getObject(execution.getKey());

            synchronized (versions) {
                execution.terminate(vote);
            }

            if (executions.get(tid).isEmpty()) {
                executions.remove(tid);
            }
        }
    }
}
