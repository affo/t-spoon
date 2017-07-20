package it.polimi.affetti.tspoon.runtime;

import java.net.Socket;

/**
 * Created by affo on 12/07/17.
 */
public abstract class ProcessRequestServer extends AbstractServer {

    @Override
    public ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(s) {
            @Override
            protected void step() throws Exception {
                String request = in.readLine();

                if (request == null) {
                    stop();
                } else {
                    parseRequest(request);
                }
            }
        };
    }

    protected abstract void parseRequest(String request);
}
