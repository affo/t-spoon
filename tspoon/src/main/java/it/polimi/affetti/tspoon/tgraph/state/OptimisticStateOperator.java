package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 25/07/17.
 */
public class OptimisticStateOperator<T, V> extends StateOperator<T, V> {
    private ReadWriteStrategy rwStrategy;

    public OptimisticStateOperator(String nameSpace, StateFunction<T, V> stateFunction, OutputTag<Update<V>> updatesTag) {
        super(nameSpace, stateFunction, updatesTag);
        switch (TransactionEnvironment.isolationLevel) {
            case PL0:
                rwStrategy = new PL0Strategy();
                break;
            case PL1:
                rwStrategy = new PL1Strategy();
                break;
            case PL2:
                rwStrategy = new PL2Strategy();
                break;
            default:
                rwStrategy = new PL3Strategy();
                break;
        }
    }

    @Override
    protected void execute(Metadata metadata, String key, Object<V> object, T element) {
        // very simple, optimistic directly executes
        registerExecution(metadata.timestamp);

        ObjectVersion<V> version = rwStrategy.extractVersion(metadata, object);
        ObjectHandler<V> handler;
        if (version.object != null) {
            handler = new ObjectHandler<>(stateFunction.copyValue(version.object));
        } else {
            handler = new ObjectHandler<>(stateFunction.defaultValue());
        }

        stateFunction.apply(element, handler);

        int lastVersion = object.getLastVersionBefore(Integer.MAX_VALUE).version;

        // add in any case, if replay it will be deleted later
        ObjectVersion<V> nextVersion = handler.object(metadata.timestamp);
        object.addVersion(nextVersion);

        metadata.vote = stateFunction.invariant(nextVersion.object) ? Vote.COMMIT : Vote.ABORT;

        if (handler.read) {
            if (!rwStrategy.isReadOK(metadata, lastVersion)) {
                metadata.vote = Vote.REPLAY;
            }
        }

        if (handler.write) {
            if (!rwStrategy.isWriteOK(metadata, lastVersion)) {
                metadata.vote = Vote.REPLAY;
            }
        }

        // add dependency
        if (TransactionEnvironment.useDependencyTracking &&
                metadata.vote == Vote.REPLAY) {
            metadata.replayCause = lastVersion;
        }

        // if the level is PL4, we should track the last timestamp that edited this record anyway. in this way
        // a downstream operator can use it to understand if t2 happened before t1 and t2 -> t1.

        collector.safeCollect(Enriched.of(metadata, element));
    }

    @Override
    protected void onTermination(int tid, Vote vote) {
        // does nothing
    }
}
