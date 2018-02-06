package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.common.Address;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by affo on 26/07/17.
 */
public class JobControlServer extends AbstractServer {
    public static final String subscribePattern = "SUBSCRIBE";
    public static final String registerPattern = "REGISTER";
    public static final String discoverPattern = "DISCOVER";

    public static final String registerFormat = registerPattern + ",%s,%s,%d";
    public static final String discoverFormat = discoverPattern + ",%s";

    private List<StringClientHandler> observers = new LinkedList<>();
    private final Map<String, Set<Address>> serversRegistry = new ConcurrentHashMap<>();

    private synchronized void subscribe(StringClientHandler handler) {
        LOG.info("Subscription received: " + handler.socket);
        observers.add(handler);
    }

    private synchronized void publish(String message) throws Exception {
        for (StringClientHandler observer : observers) {
            LOG.info("Publishing <" + message + "> to " + observer.socket);
            observer.send(message);
        }

        // auto-close on finish
        if (message.equals(JobControlObserver.finishPattern)) {
            close();
        }
    }

    private void registerForDiscovery(String label, Address address) {
        LOG.info("Registering " + label + " for discovery at " + address);
        synchronized (serversRegistry) {
            serversRegistry.computeIfAbsent(label, k -> new HashSet<>()).add(address);
            serversRegistry.notifyAll();
        }
    }

    private Set<Address> getAddresses(String label) throws InterruptedException {
        LOG.info("Discovery request for " + label);
        Set<Address> result;

        synchronized (serversRegistry) {
            do {
                result = serversRegistry.get(label);
                if (result == null) {
                    LOG.info("Waiting for registry entry: " + label);
                    serversRegistry.wait();
                }
            } while (result == null);
        }

        return result;
    }

    @Override
    protected ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(new StringClientHandler(s) {
            @Override
            protected void lifeCycle() throws Exception {
                String request = receive();

                if (request == null) {
                    throw new IOException("Request is null...");
                }

                if (request.equals(subscribePattern)) {
                    subscribe(this);
                } else if (request.startsWith(registerPattern)) {
                    String[] tokens = request.split(",");
                    String nameSpace = tokens[1];
                    String ip = tokens[2];
                    int port = Integer.parseInt(tokens[3]);
                    registerForDiscovery(nameSpace, Address.of(ip, port));
                } else if (request.startsWith(discoverPattern)) {
                    String[] tokens = request.split(",");
                    String nameSpace = tokens[1];
                    Set<String> stringAddresses = getAddresses(nameSpace).stream()
                            .map(Address::toString)
                            .collect(Collectors.toSet());
                    LOG.info("Answering discovery request for " + nameSpace + " with " + stringAddresses);
                    send(String.join(",", stringAddresses));
                } else {
                    publish(request);
                }
            }
        });
    }
}
