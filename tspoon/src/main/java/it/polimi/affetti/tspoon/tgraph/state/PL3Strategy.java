package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.OptimisticTransactionContext;

/**
 * Created by affo on 18/07/17.
 */
public class PL3Strategy extends PL2Strategy {
    @Override
    public boolean isReadOK(OptimisticTransactionContext tContext, int lastVersion) {
        return tContext.getTid() > lastVersion;
    }
}
