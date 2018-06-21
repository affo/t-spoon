package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.*;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Created by affo on 26/07/17.
 *
 * NOTE: This works only in presence of a single transactional graph
 * and with snapshots that are not overlapping
 *
 * Only one per machine
 */
public class LocalWALServer extends AbstractServer {
    // for multiple servers on the same machine (mainly for testing)
    private static int id = 0;
    private final int localWALServerID;
    private final List<FileWAL> wals;
    private final ObjectClient toProxyWAL;
    private boolean snapshotInProgress = false;
    private long inProgressWatermark = -1;
    private Thread snapshotter;

    public LocalWALServer(String proxyWALIp, int proxyWALPort) {
        this.wals = Collections.synchronizedList(new ArrayList<FileWAL>());
        this.toProxyWAL = new ObjectClient(proxyWALIp, proxyWALPort);
        this.localWALServerID = id;
        id++;
    }

    @Override
    protected void open() throws IOException {
        super.open();
        toProxyWAL.init();
        snapshotter = new Thread(new SnapshotMessageHandler(toProxyWAL));
        snapshotter.start();
    }

    @Override
    public void run() {
        // This sleep "ensures" that every CloseFunction on this machine
        // has added its WAL to this LocalWalServer before anybody from outside can
        // ask for entries replay (indeed, the server is not yet running until this
        // sleep is over).
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for WALs... Starting anyways");
        }

        super.run();
    }

    @Override
    public void close() throws Exception {
        super.close();
        snapshotter.interrupt();
        toProxyWAL.close();
    }

    public synchronized void addWAL(FileWAL wal) {
        wals.add(wal);
        notifyAll();
    }

    public static FileWAL createWAL(int localWALServerID, int tGraphID, int walID, boolean overwrite) throws IOException {
        String walName = String.format("lws%d_tg%d_%d", localWALServerID, tGraphID, walID);
        FileWAL wal = new FileWAL(walName, overwrite);
        wal.open();
        return wal;
    }

    public synchronized FileWAL addAndCreateWAL(int tGraphID, boolean overwrite) throws IOException {
        FileWAL wal = createWAL(localWALServerID, tGraphID, wals.size(), overwrite);
        addWAL(wal);
        return wal;
    }

    private void startSnapshot(long wm) throws IOException, InterruptedException {
        if (snapshotInProgress) {
            throw new IllegalStateException("Cannot start snapshot while one is in progress");
        }

        inProgressWatermark = Math.max(inProgressWatermark, wm);

        LOG.info("Snapshot starting - wm: " + inProgressWatermark);

        snapshotInProgress = true;

        LOG.info("Snapshot started, WALService replayed from " + inProgressWatermark);
    }

    public synchronized void commitSnapshot() throws IOException {
        LOG.info("Snapshot finished - wm: " + inProgressWatermark);
        for (FileWAL wal : wals) {
            wal.compact(inProgressWatermark);
        }
        snapshotInProgress = false;
    }

    // Only for testing
    Iterable<FileWAL> getWrappedWALs() {
        return wals;
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

                String strRequest = (String) request;

                Function<FileWAL, Iterator<WALEntry>> iteratorSupplier;
                if (strRequest.startsWith(ProxyWALServer.replaySourcePattern)) {
                    String[] tokens = strRequest.split(",");
                    int sourceID = Integer.valueOf(tokens[1]);
                    int numberOfSources = Integer.valueOf(tokens[2]);
                    iteratorSupplier = wal -> {
                        try {
                            return wal.replay(sourceID, numberOfSources);
                        } catch (IOException e) {
                            throw new RuntimeException("Error while replaying: " + e.getMessage());
                        }
                    };
                } else {
                    final String namespace = (String) request;
                    iteratorSupplier = wal -> {
                        try {
                            if (!namespace.equals("*")) {
                                return wal.replay(namespace);
                            }
                            return wal.replay(null); // select * for FileWAL is with null
                        } catch (IOException e) {
                            throw new RuntimeException("Error while replaying: " + e.getMessage());
                        }
                    };
                }

                for (FileWAL wal : wals) {
                    Iterator<WALEntry> entriesIterator = iteratorSupplier.apply(wal);
                    while (entriesIterator.hasNext()) {
                        send(entriesIterator.next());
                    }
                }

                send(new WALEntry(null, -1, -1, null)); // finished
                out.flush();
            }
        });
    }

    private class SnapshotMessageHandler implements Runnable {
        private final ObjectClient toWALProxy;

        public SnapshotMessageHandler(ObjectClient toWALProxy) {
            this.toWALProxy = toWALProxy;
        }

        @Override
        public void run() {
            try {
                String myIp = LocalWALServer.this.getIP();
                int myPort = LocalWALServer.this.getPort();
                toWALProxy.send(String.format(ProxyWALServer.joinFormat, myIp, myPort));

                while (true) {
                    Object request = toWALProxy.receive();

                    if (request == null) {
                        throw new IOException("Request is null...");
                    }

                    String strRequest = (String) request;

                    if (strRequest.startsWith(ProxyWALServer.startSnapshotPattern)) {
                        long newWM = Long.parseLong(strRequest.split(",")[1]);
                        startSnapshot(newWM);
                        toWALProxy.send("ACK"); // the begin phase has completed
                    }
                }
            } catch (EOFException eof) {
                LOG.error("EOF");
            } catch (IOException e) {
                LOG.error("Error while receiving from ProxyWAL: " + e.getMessage());
            } catch (InterruptedException e) {
                LOG.error("Interrupted while receiving messages from ProxyWAL");
            }
        }
    }
}

