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
    private final Map<String, List<StringClientHandler>> clientsPerKey = new HashMap<>();

    protected abstract void parseRequest(String key, String request);

    protected abstract String extractKey(String request);

    public synchronized void broadcastByKey(String key, String msg) {
        for (StringClientHandler handler : clientsPerKey.get(key)) {
            handler.send(msg);
        }
        clientsPerKey.remove(key);
    }

    @Override
    public ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(
                new StringClientHandler(s) {
                    @Override
                    protected void lifeCycle() throws Exception {
                        String request = receive();

                        String key = extractKey(request);
                        synchronized (BroadcastByKeyServer.this) {
                            clientsPerKey.computeIfAbsent(key, k -> new LinkedList<>()).add(this);
                        }
                        parseRequest(key, request);
                    }
                });
    }
}
