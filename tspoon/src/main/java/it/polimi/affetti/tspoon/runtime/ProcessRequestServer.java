package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by affo on 12/07/17.
 */
public abstract class ProcessRequestServer extends AbstractServer {

    @Override
    public ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(new StringClientHandler(s) {
            @Override
            protected void lifeCycle() throws Exception {
                String request = receive();
                if (request == null) {
                    throw new IOException("Request is null");
                }
                parseRequest(request);
            }
        });
    }

    protected abstract void parseRequest(String request);
}
