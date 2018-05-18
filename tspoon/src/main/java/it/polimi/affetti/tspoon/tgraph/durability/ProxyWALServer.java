package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.*;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Semaphore;

/**
 * Created by affo on 26/07/17.
 *
 * NOTE: This works only in presence of a single transactional graph
 * and with snapshots that are not overlapping
 */
public class ProxyWALServer extends AbstractServer {
    public static final String startSnapshotPattern = "START";
    public static final String getCurrentSnapshotWMPattern = "GET_WM";
    public static final String replaySourcePattern = "SOURCE";

    public static final String startSnapshotFormat = startSnapshotPattern + ",%d";
    public static final String replaySourceFormat = replaySourcePattern + ",%d,%d";

    private long inProgressWatermark = -1;
    private int numberOfBegins;
    private Semaphore beginSemaphore = new Semaphore(1);
    private final int numberOfSourcePartitions;
    private final String[] taskManagers;
    private SnapshotClient[] localWALServers;
    private Thread deferred;

    public ProxyWALServer(int numberOfSourcePartitions, String[] taskManagers) {
        this.numberOfSourcePartitions = numberOfSourcePartitions;
        this.taskManagers = taskManagers;
        localWALServers = new SnapshotClient[taskManagers.length];
    }

    @Override
    protected void open() throws IOException {
        super.open();
        // detached to let stuff go on
        deferred = new Thread(() -> {
            try {
                NetUtils.fillWALClients(taskManagers, localWALServers, SnapshotClient::new);
            } catch (IOException e) {
                throw new RuntimeException("Error while connecting to WALClients: " + e.getMessage());
            }
        }
        );
        deferred.start();
    }

    public void waitForOpenCompletion() throws InterruptedException {
        deferred.join();
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (int i = 0; i < localWALServers.length; i++) {
            localWALServers[i].close();
        }
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

                for (SnapshotClient cli : localWALServers) {
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
        return new LoopingClientHandler(new ObjectClientHandler(s) {
            @Override
            protected void lifeCycle() throws Exception {
                Object request = receive();

                if (request == null) {
                    throw new IOException("Request is null...");
                }

                if (request instanceof String) {
                    String strRequest = (String) request;

                    if (strRequest.startsWith(startSnapshotPattern)) {
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
        });
    }
}
