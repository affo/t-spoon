package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;

/**
 * Created by affo on 11/01/18.
 */
public class NoDependencyTrackingStrategy implements DependencyTrackingStrategy {
    @Override
    public <T> void updateDependencies(Transaction<T> transaction, Object<T> object, ObjectVersion<T> readVersion) {
        // does nothing
    }
}
