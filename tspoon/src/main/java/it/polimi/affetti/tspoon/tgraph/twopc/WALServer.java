package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ClientHandler;
import it.polimi.affetti.tspoon.runtime.LoopingClientHandler;
import it.polimi.affetti.tspoon.runtime.ObjectClientHandler;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;

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

    public static final String startSnapshotFormat = startSnapshotPattern + ",%d";


    // need to WALs to witch on snapshot
    private FileWAL currentWAL = new FileWAL("wal1.log");
    private FileWAL temporaryWAL = new FileWAL("wal2.log");

    private boolean snapshotInProgress = false;
    private long lastWatermark = -1; // for compaction
    private long temporaryWatermark;
    private int numberOfCommits;
    private final int numberOfSinkPartitions;

    public WALServer(int numberOfSinkPartitions) {
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
        if (snapshotInProgress && entry.timestamp > temporaryWatermark) {
            temporaryWAL.addEntry(entry);
        }
    }

    private synchronized void startSnapshot(long wm) throws IOException {
        Preconditions.checkArgument(wm > lastWatermark);
        temporaryWatermark = wm;
        snapshotInProgress = true;

        // save every new entry in the temporary
        Iterator<WAL.Entry> replaying = currentWAL.replay(null);
        while (replaying.hasNext()) {
            WAL.Entry next = replaying.next();
            if (next.timestamp > wm) {
                temporaryWAL.addEntry(next);
            }
        }
    }

    private synchronized void commitSnapshot() throws IOException {
        numberOfCommits++;

        if (numberOfCommits == numberOfSinkPartitions) {
            lastWatermark = temporaryWatermark;
            snapshotInProgress = false;
            numberOfCommits = 0;

            // switchFile
            FileWAL tmp = currentWAL;
            currentWAL = temporaryWAL;
            temporaryWAL = tmp;
            temporaryWAL.clear();
        }

    }

    private synchronized void resetSnapshot() {
        snapshotInProgress = false;
        numberOfCommits = 0;
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
                        return;
                    } else if (strRequest.startsWith(commitSnapshotPattern)) {
                        commitSnapshot();
                        return;
                    } else if (strRequest.startsWith(getCurrentSnapshotWMPattern)) {
                        send(temporaryWatermark);
                        return;
                    }

                    // someone wants replay
                    // if a snapshot was open it has not happened indeed...
                    resetSnapshot();

                    String namespace = (String) request;
                    if (namespace.equals("*")) {
                        namespace = null; // select all for FileWAL is with null
                    }
                    Iterator<WAL.Entry> iterator = currentWAL.replay(namespace);
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
