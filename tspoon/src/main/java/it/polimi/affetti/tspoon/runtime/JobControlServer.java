package it.polimi.affetti.tspoon.runtime;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 26/07/17.
 */
public class JobControlServer extends AbstractServer {
    public static final String subscribePattern = "SUBSCRIBE";
    private List<ClientHandler> observers = new LinkedList<>();

    private synchronized void subscribe(ClientHandler handler) {
        observers.add(handler);
    }

    private synchronized void publish(String message) {
        for (ClientHandler observer : observers) {
            LOG.info("Publishing " + message + " to " + observer.socket);
            observer.send(message);
        }
    }

    @Override
    protected ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(s) {
            @Override
            protected void step() throws Exception {
                String request = receive();

                if (request == null) {
                    stop();
                } else {
                    if (request.equals(subscribePattern)) {
                        LOG.info("Subscription received: " + this.socket.toString());
                        subscribe(this);
                    } else {
                        LOG.info("Publishing " + request + "...");
                        publish(request);
                    }
                }
            }
        };
    }
}
