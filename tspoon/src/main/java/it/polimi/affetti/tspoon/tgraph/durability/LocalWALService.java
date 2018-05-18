package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ClientHandler;
import it.polimi.affetti.tspoon.runtime.LoopingClientHandler;
import it.polimi.affetti.tspoon.runtime.ObjectClientHandler;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;

/**
 * Created by affo on 26/07/17.
 *
 * NOTE: This works only in presence of a single transactional graph
 * and with snapshots that are not overlapping
 *
 * Only one per machine
 */
public class LocalWALService extends AbstractServer {
    public static final String WAL_FNAME = "wal.log";
    public static final String WAL_TMP_FNAME = "wal_tmp.log";
    public static final String WAL_SWITCH_FNAME = ".wal_switch.log";

    // need to WALs to witch on snapshot
    private FileWAL currentWAL;
    private FileWAL temporaryWAL;

    private boolean snapshotInProgress = false;
    private long inProgressWatermark = -1;
    private final String filePrefix;

    // If restored the files will be not overwritten until recovery complete!
    public LocalWALService(boolean restored) {
        this(restored, "");
    }

    /**
     *
     * @param restored
     * @param filePrefix differentiate for multiple TMs on the same machine
     */
    public LocalWALService(boolean restored, String filePrefix) {
        this.filePrefix = filePrefix;
        currentWAL = new FileWAL(filePrefix + WAL_FNAME, !restored);
        temporaryWAL = new FileWAL(filePrefix + WAL_TMP_FNAME, !restored);
    }

    @Override
    protected synchronized void open() throws IOException {
        super.open();
        currentWAL.open();
        temporaryWAL.open();
    }

    /**
     * Could close from various closeFunctions
     * @throws Exception
     */
    @Override
    public synchronized void close() throws Exception {
        if (currentWAL != null && temporaryWAL != null) {
            super.close();
            currentWAL.close();
            temporaryWAL.close();
            currentWAL = null;
            temporaryWAL = null;
        }
    }

    public synchronized void addEntry(WALEntry entry) throws IOException {
        currentWAL.addEntry(entry);
        if (snapshotInProgress && entry.timestamp > inProgressWatermark) {
            temporaryWAL.addEntry(entry);
        }
    }

    /**
     * delegates to FileWAL
     * @param namespace
     * @return
     * @throws IOException
     */
    public synchronized Iterator<WALEntry> replay(String namespace) throws IOException {
        return currentWAL.replay(namespace);
    }

    /**
     * Delegates to FileWAL
     * @param sourceID
     * @param numberOfSources
     * @return
     * @throws IOException
     */
    public synchronized Iterator<WALEntry> replay(int sourceID, int numberOfSources) throws IOException {
        return currentWAL.replay(sourceID, numberOfSources);
    }

    private void startSnapshot(long wm) throws IOException, InterruptedException {
        if (snapshotInProgress) {
            abortSnapshot();
        }

        inProgressWatermark = Math.max(inProgressWatermark, wm);

        LOG.info("Snapshot starting - wm: " + inProgressWatermark);

        snapshotInProgress = true;

        // save every new entry in the temporary
        Iterator<WALEntry> replaying = currentWAL.replay(null);
        while (replaying.hasNext()) {
            WALEntry next = replaying.next();
            if (next.timestamp > inProgressWatermark) {
                temporaryWAL.addEntry(next);
            }
        }

        LOG.info("Snapshot started, WAL replayed from " + inProgressWatermark);
    }

    public synchronized void commitSnapshot() throws IOException {
        LOG.info("Snapshot finished - wm: " + inProgressWatermark);
        resetSnapshot();
        switchFile();
    }

    // TODO what happens if failure while switching file??
    private synchronized void switchFile() throws IOException {
        // change names... not atomical
        currentWAL.rename(filePrefix + WAL_SWITCH_FNAME);
        temporaryWAL.rename(filePrefix + WAL_FNAME);
        currentWAL.rename(filePrefix + WAL_TMP_FNAME);

        // change vars
        FileWAL tmp = currentWAL;
        currentWAL = temporaryWAL;
        temporaryWAL = tmp;
        temporaryWAL.clear();
    }

    private synchronized void abortSnapshot() throws IOException {
        LOG.warn("Aborting current snapshot: " + inProgressWatermark);
        resetSnapshot();
        temporaryWAL.clear();
    }

    private synchronized void resetSnapshot() {
        snapshotInProgress = false;
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

                String strRequest = (String) request;

                if (strRequest.startsWith(ProxyWALServer.startSnapshotPattern)) {
                    long newWM = Long.parseLong(strRequest.split(",")[1]);
                    startSnapshot(newWM);
                    send("ACK"); // the begin phase has completed
                    return;
                }

                Iterator<WALEntry> iterator;
                if (strRequest.startsWith(ProxyWALServer.replaySourcePattern)) {
                    String[] tokens = strRequest.split(",");
                    int sourceID = Integer.valueOf(tokens[1]);
                    int numberOfSources = Integer.valueOf(tokens[2]);
                    iterator = replay(sourceID, numberOfSources);
                } else {
                    String namespace = (String) request;
                    if (namespace.equals("*")) {
                        namespace = null; // select * for FileWAL is with null
                    }
                    iterator = replay(namespace);
                }

                while (iterator.hasNext()) {
                    send(iterator.next());
                }
                send(new WALEntry(null, -1, -1, null)); // finished
            }
        });
    }
}

