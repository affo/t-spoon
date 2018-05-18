package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.shaded.com.google.common.io.Files;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

/**
 * Created by affo on 31/07/17.
 */
public class FileWAL {
    public static final String WAL_DIR = "wals/";
    public static final String MAIN_WAL_SUFFIX = "_wal.log";
    public static final String SLAVE_WAL_SUFFIX = "_swal.log";
    public static final String TMP_WAL_SUFFIX = "_wal.log.tmp";

    private String mainName, slaveName, tmpName;
    private final boolean overwrite;
    private File mainWAL, slaveWal, tmpWal;
    private ObjectOutputStream mainOut, slaveOut, tmpOut;
    private boolean compactionRunning = false;
    private BlockingQueue<WALEntry> pendingEntries = new LinkedBlockingQueue<>();

    private ExecutorService pool = Executors.newCachedThreadPool();

    public FileWAL(String id, boolean overwrite) {
        this.mainName = WAL_DIR + id + MAIN_WAL_SUFFIX;
        this.slaveName = WAL_DIR + id + SLAVE_WAL_SUFFIX;
        this.tmpName = WAL_DIR + id + TMP_WAL_SUFFIX;
        this.overwrite = overwrite;
    }

    public void open() throws IOException {
        mainWAL = new File(mainName);
        slaveWal = new File(slaveName);
        tmpWal = new File(tmpName);

        mainWAL.getParentFile().mkdirs();
        mainWAL.createNewFile();
        slaveWal.createNewFile();
        tmpWal.createNewFile();
        // if overwrite, then do not append
        mainOut = new ObjectOutputStream(new FileOutputStream(mainWAL, !overwrite));
        // TODO should check if there was a failure while swapping files...
        // TODO for simplicity, we assume it is an atomic operation
        slaveOut = new ObjectOutputStream(new FileOutputStream(slaveWal, false));
        tmpOut = new ObjectOutputStream(new FileOutputStream(tmpWal, false));
    }

    public void close() throws IOException {
        mainOut.close();
        slaveOut.close();
        tmpOut.close();
    }

    private void clearSlaveAndTmp() throws IOException {
        new PrintWriter(slaveOut).close();
        new PrintWriter(tmpOut).close();
    }

    public synchronized void compact(long timestamp) throws IOException {
        compactionRunning = true;
        pool.submit(() -> {
            try {
                // write everything after the timestamp
                Iterator<WALEntry> replay = replay(e -> e.timestamp > timestamp);
                while (replay.hasNext()) {
                    tmpOut.writeObject(replay.next());
                }
                tmpOut.flush();
                tmpOut.reset();

                drainPending();
            } catch (IOException ioe) {
                throw new RuntimeException("IOE while compacting: " + ioe.getMessage());
            }
        });
    }

    private synchronized void drainPending() throws IOException {
        for (WALEntry entry : pendingEntries) {
            // write them on tmp
            tmpOut.writeObject(entry);
        }
        tmpOut.flush();
        tmpOut.reset();
        pendingEntries.clear();

        Files.copy(tmpWal, mainWAL);
        clearSlaveAndTmp();
        compactionRunning = false;
        notifyAll();
    }

    public synchronized void addEntry(WALEntry entry) {
        ObjectOutputStream os;
        if (compactionRunning) {
            os = slaveOut;
            pendingEntries.add(entry);
        } else {
            os = mainOut;
        }

        try {
            os.writeObject(entry);
            os.flush();
            os.reset();
        } catch (IOException e) {
            // make it crash, we cannot avoid persisting the WALService
            throw new RuntimeException("Cannot persist to WALService");
        }
    }

    /**
     * If namespace is null it returns every entry, no matter the namespace
     * @param namespace
     * @return
     * @throws IOException
     */
    public Iterator<WALEntry> replay(String namespace) throws IOException {
        waitForCompactionFinish();
        return replay(e -> (namespace == null || e.updates.isInvolved(namespace)));
    }

    // cache the unit
    private int unit = -1;

    /**
     * Only the entries for the provided source ID
     * @param sourceID
     * @param numberOfSources
     * @return
     * @throws IOException
     */
    public Iterator<WALEntry> replay(int sourceID, int numberOfSources) throws IOException {
        waitForCompactionFinish();
        if (unit < 0) {
            unit = TimestampGenerator.calcUnit(numberOfSources);
        }
        return replay(e -> TimestampGenerator.checkTimestamp(sourceID, e.timestamp, unit));
    }

    private synchronized void waitForCompactionFinish() {
        while (compactionRunning) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while compacting");
            }
        }
    }

    private Iterator<WALEntry> replay(Predicate<WALEntry> predicate) throws IOException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(mainWAL));

        try {
            List<WALEntry> entries = new ArrayList<>();
            while (true) {
                try {
                    WALEntry e = (WALEntry) in.readObject();
                    if (e.vote == Vote.COMMIT
                            && e.updates != null && predicate.test(e)) {
                        entries.add(e);
                    }
                } catch (EOFException eof) {
                    break;
                }
            }

            return entries.iterator();
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Cannot recover from WALService: " + ex.getMessage());
        } finally {
            in.close();
        }
    }
}
