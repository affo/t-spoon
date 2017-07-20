package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.tgraph.OptimisticTransactionContext;
import it.polimi.affetti.tspoon.tgraph.TransactionContext;

/**
 * Created by affo on 14/07/17.
 */
public class OptimisticOpenOperator<T> extends OpenOperator<T> {
    @Override
    protected TransactionContext getFreshTransactionContext() {
        return new OptimisticTransactionContext();
    }

    @Override
    protected AbstractServer getOpenServer(SafeCollector<T, Integer> collector) {
        return new OpenServer(collector);
    }

    // TODO impl and use collector for watermark output
    private class OpenServer extends ProcessRequestServer {
        private SafeCollector<T, Integer> collector;

        public OpenServer(SafeCollector<T, Integer> collector) {
            this.collector = collector;
        }

        @Override
        protected void parseRequest(String request) {
            LOG.info(request);
        }
    }
}
