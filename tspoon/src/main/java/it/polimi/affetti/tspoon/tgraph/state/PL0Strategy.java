package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.OptimisticTransactionContext;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 20/07/17.
 */
public class PL0Strategy implements ReadWriteStrategy {
    @Override
    public boolean isReadOK(OptimisticTransactionContext tContext, int lastVersion) {
        return true;
    }

    @Override
    public <V> ObjectVersion<V> extractVersion(OptimisticTransactionContext tContext, Object<V> versions) {
        return versions.getLastVersionBefore(Integer.MAX_VALUE);
    }

    @Override
    public boolean isWriteOK(OptimisticTransactionContext tContext, int lastVersion) {
        return true;
    }
}
