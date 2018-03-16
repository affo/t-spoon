package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;

/**
 * Created by affo on 11/01/18.
 */
public class StandardDependencyTrackingStrategy implements DependencyTrackingStrategy {
    @Override
    public <T> void updateDependencies(Transaction<T> transaction, Object<T> object, int version, int createdBy) {
        if (transaction.vote == Vote.REPLAY) {
            transaction.addDependency(createdBy);
        }
    }
}
