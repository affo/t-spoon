package it.polimi.affetti.tspoon.runtime;

import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 24/07/17.
 */
public abstract class BroadcastByKeyServer extends AbstractServer {
    private final Map<String, List<ClientHandler>> clientsPerKey = new HashMap<>();

    protected abstract void parseRequest(String key, String request);

    protected abstract String extractKey(String request);

    public synchronized void broadcastByKey(String key, String msg) {
        for (ClientHandler handler : clientsPerKey.get(key)) {
            handler.send(msg);
        }
        clientsPerKey.remove(key);
    }

    @Override
    public ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(s) {
            @Override
            protected void step() throws Exception {
                String request = receive();

                if (request == null) {
                    stop();
                } else {
                    String key = extractKey(request);
                    synchronized (BroadcastByKeyServer.this) {
                        List<ClientHandler> clients = clientsPerKey.computeIfAbsent(key, k -> new LinkedList<>());
                        clients.add(this);
                    }
                    parseRequest(key, request);
                }
            }
        };
    }
}
