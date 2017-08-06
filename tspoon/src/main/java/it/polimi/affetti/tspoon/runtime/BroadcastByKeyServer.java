package it.polimi.affetti.tspoon.runtime;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by affo on 24/07/17.
 */
public abstract class BroadcastByKeyServer extends AbstractServer {
    private final Map<String, List<StringClientHandler>> clientsPerKey = new ConcurrentHashMap<>();

    protected abstract void parseRequest(String key, String request);

    protected abstract String extractKey(String request);

    public void broadcastByKey(String key, String msg) {
        List<StringClientHandler> clients = clientsPerKey.remove(key);
        for (StringClientHandler handler : clients) {
            handler.send(msg);
        }

    }

    @Override
    public ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(
                new StringClientHandler(s) {
                    @Override
                    protected void lifeCycle() throws Exception {
                        String request = receive();

                        String key = extractKey(request);
                        synchronized (clientsPerKey) {
                            clientsPerKey.computeIfAbsent(key, k -> new LinkedList<>()).add(this);
                        }
                        parseRequest(key, request);
                    }
                });
    }
}
