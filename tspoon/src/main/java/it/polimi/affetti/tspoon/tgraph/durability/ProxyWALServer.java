package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ClientHandler;
import it.polimi.affetti.tspoon.runtime.ObjectClientHandler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Created by affo on 26/07/17.
 *
 * NOTE: This works only in presence of a single transactional graph
 * and with snapshots that are not overlapping
 */
public class ProxyWALServer extends AbstractServer {
    public static final String joinPattern = "JOIN";
    public static final String startSnapshotPattern = "START";
    public static final String getCurrentSnapshotWMPattern = "GET_WM";
    public static final String replaySourcePattern = "SOURCE";

    public static final String joinFormat = joinPattern + ",%s,%d";
    public static final String startSnapshotFormat = startSnapshotPattern + ",%d";
    public static final String replaySourceFormat = replaySourcePattern + ",%d,%d";

    private long inProgressWatermark = -1;
    private int numberOfBegins;
    private Semaphore beginSemaphore = new Semaphore(1);
    private final int numberOfSourcePartitions;
    private final String[] taskManagers;
    // wait for the first taskManagers.length that answer to requests
    // in order to handle failures!
    private Map<Address, SnapshotClient> walServers;

    public ProxyWALServer(int numberOfSourcePartitions, String[] taskManagers) {
        this.numberOfSourcePartitions = numberOfSourcePartitions;
        this.taskManagers = taskManagers;
        this.walServers = new HashMap<>();
    }

    /**
     * Debug & testing
     * @throws InterruptedException
     */
    protected synchronized void waitForJoinCompletion() throws InterruptedException {
        while (walServers.size() < taskManagers.length) {
            wait();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (SnapshotClient cli : walServers.values()) {
            cli.close();
        }
    }

    private synchronized void join(String ip, int port,
                                   Socket socket, ObjectInputStream in, ObjectOutputStream out) throws IOException {
        Address address = Address.of(ip, port);
        SnapshotClient snapshotClient = walServers.get(address);
        if (snapshotClient != null) {
            snapshotClient.close();
        }

        snapshotClient = new SnapshotClient(socket, in, out);
        walServers.put(address, snapshotClient);
        notifyAll();
        LOG.info(">>> " + ip + ":" + port + " has joined the proxy");
    }

    private void startSnapshot(long wm) throws IOException, InterruptedException {
        synchronized (this) {
            if (numberOfBegins == 0) {
                inProgressWatermark = -1;
                beginSemaphore.drainPermits();
            }

            inProgressWatermark = Math.max(inProgressWatermark, wm);
            numberOfBegins++;

            LOG.info("Snapshot starting - begins: " + numberOfBegins + "/" + numberOfSourcePartitions + " wm: " + inProgressWatermark);

            if (numberOfBegins == numberOfSourcePartitions) {
                numberOfBegins = 0;

                for (SnapshotClient cli : walServers.values()) {
                    cli.startSnapshot(inProgressWatermark);
                }

                LOG.info("Snapshot started: WM " + inProgressWatermark);
                beginSemaphore.release();
            }
        }

        beginSemaphore.acquire();
        beginSemaphore.release();
    }

    @Override
    protected ClientHandler getHandlerFor(Socket s) {
        return new ObjectClientHandler(s) {
            private boolean stop = false;

            @Override
            public void close() throws IOException {
                stop = true;
                super.close();
            }

            @Override
            protected void lifeCycle() throws Exception {
                while (!stop) {
                    Object request = receive();

                    if (request == null) {
                        throw new IOException("Request is null...");
                    }

                    if (request instanceof String) {
                        String strRequest = (String) request;

                        // when a local server signals its existence and
                        // becomes reachable
                        if (strRequest.startsWith(joinPattern)) {
                            String[] tokens = strRequest.split(",");
                            String ip = tokens[1];
                            int port = Integer.parseInt(tokens[2]);
                            join(ip, port, socket, in, out);
                            // this channel will be used by this proxy to communicate
                            // towards the localWALServer. The handler
                            // can end up its job, because it won't receive any other message
                            // (except for answers to startSnap)...
                            break;
                        } else if (strRequest.startsWith(startSnapshotPattern)) {
                            long newWM = Long.parseLong(strRequest.split(",")[1]);
                            startSnapshot(newWM);
                            send("ACK"); // the begin phase has globally completed
                        } else if (strRequest.startsWith(getCurrentSnapshotWMPattern)) {
                            // if the init phase is still running, wait for its completion
                            beginSemaphore.acquire();
                            beginSemaphore.release();
                            send(inProgressWatermark);
                        } else {
                            throw new IllegalArgumentException("[ERROR] illegal request to ProxyWALServer: " + request);
                        }
                    } else {
                        throw new RuntimeException("[ERROR] in request to ProxyWALServer: " + request);
                    }
                }
            }
        };
    }
}
