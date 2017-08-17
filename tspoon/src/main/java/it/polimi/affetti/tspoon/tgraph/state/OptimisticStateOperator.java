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
    private VersioningStrategy versioningStrategy;

    public OptimisticStateOperator(String nameSpace, StateFunction<T, V> stateFunction, OutputTag<Update<V>> updatesTag) {
        super(nameSpace, stateFunction, updatesTag);
        switch (TransactionEnvironment.isolationLevel) {
            case PL0:
                versioningStrategy = new PL0Strategy();
                break;
            case PL1:
                versioningStrategy = new PL1Strategy();
                break;
            case PL2:
                versioningStrategy = new PL2Strategy();
                break;
            case PL4:
                versioningStrategy = new PL4Strategy();
                break;
            default:
                versioningStrategy = new PL3Strategy();
                break;
        }
    }

    @Override
    protected void execute(Metadata metadata, String key, Object<V> object, T element) {
        // very simple, optimistic directly executes
        ObjectVersion<V> version = versioningStrategy.extractObjectVersion(metadata, object);
        ObjectHandler<V> handler;
        if (version.object != null) {
            handler = new ObjectHandler<>(stateFunction.copyValue(version.object));
        } else {
            handler = new ObjectHandler<>(stateFunction.defaultValue());
        }

        stateFunction.apply(element, handler);

        ObjectVersion<V> nextVersion = handler.object(versioningStrategy.getVersionIdentifier(metadata));

        metadata.vote = stateFunction.invariant(nextVersion.object) ? Vote.COMMIT : Vote.ABORT;

        if (handler.write) {
            if (!versioningStrategy.isWritingAllowed(metadata, object)) {
                metadata.vote = Vote.REPLAY;
            }
        }

        // add dependency
        if (TransactionEnvironment.useDependencyTracking) {
            metadata.dependencyTracking.addAll(versioningStrategy.extractDependencies(metadata, object));
        }

        // add in any case, if replay it will be deleted later
        object.addVersion(nextVersion);

        collector.safeCollect(Enriched.of(metadata, element));
    }

    @Override
    protected void onTermination(int tid, Vote vote) {
        // does nothing
    }
}
