package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ClientHandler;
import it.polimi.affetti.tspoon.runtime.LoopingClientHandler;
import it.polimi.affetti.tspoon.runtime.ObjectClientHandler;

import java.net.Socket;

/**
 * Created by affo on 02/08/17.
 */
public class QueryServer extends AbstractServer {
    private QueryListener queryListener;

    public QueryServer(QueryListener queryListener) {
        this.queryListener = queryListener;
    }

    @Override
    protected ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(
                new ObjectClientHandler(s) {
                    @Override
                    protected void lifeCycle() throws Exception {
                        Query query = (Query) receive();
                        send(queryListener.onQuery(query));
                    }
                });
    }
}
