package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.OptimisticTransactionContext;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.io.Serializable;

/**
 * Created by affo on 18/07/17.
 */
public interface ReadWriteStrategy extends Serializable {
    boolean isReadOK(OptimisticTransactionContext tContext, int lastVersion);

    <V> ObjectVersion<V> extractVersion(OptimisticTransactionContext tContext, Object<V> versions);

    boolean isWriteOK(OptimisticTransactionContext tContext, int lastVersion);
}
