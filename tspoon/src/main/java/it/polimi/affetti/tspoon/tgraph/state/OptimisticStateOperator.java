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
    protected void execute(TransactionContext tContext, String key, Object<V> object, Metadata metadata, T element) {
        // very simple. Optimistic directly executes
        ObjectVersion<V> version = versioningStrategy.extractObjectVersion(metadata, object);
        ObjectHandler<V> handler;
        if (version.object != null) {
            handler = new ObjectHandler<>(stateFunction.copyValue(version.object));
        } else {
            handler = new ObjectHandler<>(stateFunction.defaultValue());
        }

        stateFunction.apply(element, handler);

        int versionId = versioningStrategy.getVersionIdentifier(metadata);
        tContext.version = versionId;

        ObjectVersion<V> nextVersion = handler.object(versionId);

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

        // avoid wasting memory in case we generated an invalid version
        if (metadata.vote != Vote.REPLAY) {
            object.addVersion(nextVersion);
        }

        collector.safeCollect(Enriched.of(metadata, element));
    }

    @Override
    protected void onTermination(int tid, Vote vote) {
        // does nothing
    }
}
