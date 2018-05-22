package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.*;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
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
    private final FileWAL[] wals;
    private final ObjectClient toProxyWAL;
    private int index = 0;
    private boolean snapshotInProgress = false;
    private long inProgressWatermark = -1;
    private Thread snapshotter;

    public LocalWALServer(int numberOfWALs, String proxyWALIp, int proxyWALPort) {
        this.wals = new FileWAL[numberOfWALs];
        this.toProxyWAL = new ObjectClient(proxyWALIp, proxyWALPort);
    }

    @Override
    protected void open() throws IOException {
        super.open();
        toProxyWAL.init();
        snapshotter = new Thread(new SnapshotMessageHandler(toProxyWAL));
        snapshotter.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        snapshotter.interrupt();
        toProxyWAL.close();
    }

    public synchronized void addWAL(FileWAL wal) {
        wals[index++] = wal;
        notifyAll();
    }

    private synchronized void waitForWALs() throws InterruptedException {
        while (index < wals.length) {
            wait();
        }
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
    FileWAL[] getWrappedWALs() {
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

                waitForWALs();

                // replay all files
                for (FileWAL wal : wals) {
                    Iterator<WALEntry> iterator = iteratorSupplier.apply(wal);
                    while (iterator.hasNext()) {
                        send(iterator.next());
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

