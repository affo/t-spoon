package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ClientHandler;
import it.polimi.affetti.tspoon.runtime.LoopingClientHandler;
import it.polimi.affetti.tspoon.runtime.ObjectClientHandler;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.Semaphore;

/**
 * Created by affo on 26/07/17.
 *
 * NOTE: This works only in presence of a single transactional graph
 * and with snapshots that are not overlapping
 */
public class WALServer extends AbstractServer {
    public static final String startSnapshotPattern = "START";
    public static final String commitSnapshotPattern = "COMMIT";
    public static final String getCurrentSnapshotWMPattern = "GET_WM";
    public static final String replaySourcePattern = "SOURCE";

    public static final String startSnapshotFormat = startSnapshotPattern + ",%d";
    public static final String replaySourceFormat = replaySourcePattern + ",%d,%d";


    // need to WALs to witch on snapshot
    private FileWAL currentWAL = new FileWAL("wal1.log");
    private FileWAL temporaryWAL = new FileWAL("wal2.log");

    private boolean snapshotInProgress = false;
    private long inProgressWatermark = -1;
    private int numberOfBegins, numberOfCommits;
    private Semaphore beginSemaphore = new Semaphore(0);
    private final int numberOfSourcePartitions, numberOfSinkPartitions;

    public WALServer(int numberOfSourcePartitions, int numberOfSinkPartitions) {
        this.numberOfSourcePartitions = numberOfSourcePartitions;
        this.numberOfSinkPartitions = numberOfSinkPartitions;
    }

    @Override
    protected void open() throws IOException {
        super.open();
        currentWAL.open();
        temporaryWAL.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        currentWAL.close();
        temporaryWAL.close();
    }

    private synchronized void addEntry(WAL.Entry entry) throws IOException {
        currentWAL.addEntry(entry);
        if (snapshotInProgress && entry.timestamp > inProgressWatermark) {
            temporaryWAL.addEntry(entry);
        }
    }

    private void startSnapshot(long wm) throws IOException, InterruptedException {
        if (snapshotInProgress) {
            abortSnapshot();
        }

        synchronized (this) {
            if (numberOfBegins == 0) {
                inProgressWatermark = -1;
            }

            inProgressWatermark = Math.max(inProgressWatermark, wm);
            numberOfBegins++;

            LOG.info("Snapshot starting - begins: " + numberOfBegins + "/" + numberOfSourcePartitions + " wm: " + inProgressWatermark);

            if (numberOfBegins == numberOfSourcePartitions) {
                numberOfBegins = 0;
                snapshotInProgress = true;

                // save every new entry in the temporary
                Iterator<WAL.Entry> replaying = currentWAL.replay(null);
                while (replaying.hasNext()) {
                    WAL.Entry next = replaying.next();
                    if (next.timestamp > inProgressWatermark) {
                        temporaryWAL.addEntry(next);
                    }
                }

                LOG.info("Snapshot started, WAL replayed until " + inProgressWatermark);
                beginSemaphore.release();
            }
        }

        beginSemaphore.acquire();
        beginSemaphore.release();
    }

    private synchronized void commitSnapshot() throws IOException {
        numberOfCommits++;

        if (numberOfCommits == numberOfSinkPartitions) {
            LOG.info("Snapshot finished - wm: " + inProgressWatermark);
            resetSnapshot();

            // switchFile
            FileWAL tmp = currentWAL;
            currentWAL = temporaryWAL;
            temporaryWAL = tmp;
            temporaryWAL.clear();
        }

    }

    private synchronized void abortSnapshot() throws IOException {
        LOG.warn("Aborting current snapshot: " + inProgressWatermark);
        resetSnapshot();
        temporaryWAL.clear();
    }

    private synchronized void resetSnapshot() {
        snapshotInProgress = false;
        numberOfCommits = 0;
        beginSemaphore.drainPermits(); // next snapshot will start from scratch
    }

    // Only for testing
    FileWAL getWrappedWAL() {
        return currentWAL;
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

                if (request instanceof WAL.Entry) {
                    WAL.Entry entry = (WAL.Entry) request;

                    // multiple clients write in mutual exclusion on the WAL
                    addEntry(entry);
                    send("ACK"); // need to make the client wait until persistence is done
                } else if (request instanceof String) {
                    String strRequest = (String) request;

                    if (strRequest.startsWith(startSnapshotPattern)) {
                        long newWM = Long.parseLong(strRequest.split(",")[1]);
                        startSnapshot(newWM);
                        send("ACK"); // the begin phase has completed
                        return;
                    } else if (strRequest.startsWith(commitSnapshotPattern)) {
                        commitSnapshot();
                        return;
                    } else if (strRequest.startsWith(getCurrentSnapshotWMPattern)) {
                        // if the init phase is still running, wait for its completion
                        beginSemaphore.acquire();
                        beginSemaphore.release();
                        send(inProgressWatermark);
                        return;
                    }

                    Iterator<WAL.Entry> iterator;
                    if (strRequest.startsWith(replaySourcePattern)) {
                        String[] tokens = strRequest.split(",");
                        int sourceID = Integer.valueOf(tokens[1]);
                        int numberOfSources = Integer.valueOf(tokens[2]);
                        iterator = currentWAL.replay(sourceID, numberOfSources);
                    } else {
                        String namespace = (String) request;
                        if (namespace.equals("*")) {
                            namespace = null; // select * for FileWAL is with null
                        }
                        iterator = currentWAL.replay(namespace);
                    }

                    while (iterator.hasNext()) {
                        send(iterator.next());
                    }
                    send(new WAL.Entry(null, -1, -1, null)); // finished
                } else {
                    throw new RuntimeException("[ERROR] in request to WALServer: " + request);
                }
            }
        });
    }
}
