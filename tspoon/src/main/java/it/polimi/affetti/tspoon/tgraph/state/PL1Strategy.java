package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.OptimisticTransactionContext;

/**
 * Created by affo on 18/07/17.
 */
public class PL1Strategy extends PL0Strategy {
    @Override
    public boolean isWriteOK(OptimisticTransactionContext tContext, int lastVersion) {
        return tContext.getTid() > lastVersion;
    }
}
