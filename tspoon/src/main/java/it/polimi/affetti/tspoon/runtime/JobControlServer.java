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
    private final Map<String, Set<Address>> queryServersRegistry = new ConcurrentHashMap<>();

    private synchronized void subscribe(StringClientHandler handler) {
        LOG.info("Subscription received: " + handler.socket);
        observers.add(handler);
    }

    private synchronized void publish(String message) throws IOException {
        for (StringClientHandler observer : observers) {
            LOG.info("Publishing " + message + " to " + observer.socket);
            observer.send(message);
        }

        // auto-close on finish
        if (message.equals(JobControlClient.finishPattern)) {
            close();
        }
    }

    private void registerForDiscovery(String nameSpace, Address address) {
        LOG.info("Registering " + nameSpace + " for discovery at " + address);
        synchronized (queryServersRegistry) {
            queryServersRegistry.computeIfAbsent(nameSpace, k -> new HashSet<>()).add(address);
            queryServersRegistry.notifyAll();
        }
    }

    private Set<Address> getAddressesForQueryServer(String nameSpace) throws InterruptedException {
        LOG.info("Discovery request for " + nameSpace);
        Set<Address> result;
        synchronized (queryServersRegistry) {
            result = queryServersRegistry.get(nameSpace);
            while (result == null) {
                queryServersRegistry.wait();
                result = queryServersRegistry.get(nameSpace);
            }
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
                    Set<String> stringAddresses = getAddressesForQueryServer(nameSpace)
                            .stream().map(Address::toString).collect(Collectors.toSet());
                    send(String.join(",", stringAddresses));
                } else {
                    LOG.info("Publishing " + request + "...");
                    publish(request);
                }
            }
        });
    }
}
