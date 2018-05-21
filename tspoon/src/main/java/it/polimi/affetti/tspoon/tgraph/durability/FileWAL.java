package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
    private File mainWAL, slaveWAL, tmpWAL;
    private ObjectOutputStream mainOut, slaveOut, tmpOut;
    private boolean compactionRunning = false;
    private BlockingQueue<WALEntry> pendingEntries = new LinkedBlockingQueue<>();
    private List<WALEntry> walContent;
    private long loadWALTime;

    private ExecutorService pool = Executors.newFixedThreadPool(1);

    public FileWAL(String id, boolean overwrite) {
        this.mainName = WAL_DIR + id + MAIN_WAL_SUFFIX;
        this.slaveName = WAL_DIR + id + SLAVE_WAL_SUFFIX;
        this.tmpName = WAL_DIR + id + TMP_WAL_SUFFIX;
        this.overwrite = overwrite;
    }

    public void open() throws IOException {
        mainWAL = new File(mainName);
        slaveWAL = new File(slaveName);
        tmpWAL = new File(tmpName);

        mainWAL.getParentFile().mkdirs();
        mainWAL.createNewFile();
        slaveWAL.createNewFile();
        tmpWAL.createNewFile();

        // if overwrite, then do not append
        FileOutputStream fos = new FileOutputStream(mainWAL, !overwrite);
        if (overwrite) {
            // new file, we need a header at the beginning
            mainOut = new ObjectOutputStream(fos);
        } else {
            // appending, we MUST NOT write the header
            mainOut = new NoHeaderObjectOutputStream(fos);
        }
        // TODO should check if there was a failure while swapping files...
        // TODO for simplicity, we assume it is an atomic operation
        // always need a header when creating new file
        slaveOut = new ObjectOutputStream(new FileOutputStream(slaveWAL, false));
        tmpOut = new ObjectOutputStream(new FileOutputStream(tmpWAL, false));

        this.loadWALTime = forceReload();
    }

    public void close() throws IOException {
        mainOut.close();
        slaveOut.close();
        tmpOut.close();
    }

    public long getLoadWALTime() {
        return loadWALTime;
    }

    private void resetTemporaryWALs() throws IOException {
        slaveOut = new ObjectOutputStream(new FileOutputStream(slaveWAL, false));
        tmpOut = new ObjectOutputStream(new FileOutputStream(tmpWAL, false));
    }

    public synchronized void compact(long timestamp) throws IOException {
        compactionRunning = true;
        pool.submit(() -> {
            try {
                // write everything after the timestamp
                List<WALEntry> entries = loadWAL();
                for (WALEntry entry : entries) {
                    if (entry.timestamp > timestamp) {
                        tmpOut.writeObject(entry);
                    }
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

        tmpWAL.renameTo(mainWAL);
        tmpWAL.createNewFile();

        // NOTE:
        // tmp now points to main, if we want to still write to main,
        // we should use tmp stream
        mainOut = tmpOut;
        resetTemporaryWALs();

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
        waitForCompactionToFinish();
        return replay(e -> (namespace == null || e.updates.isInvolved(namespace))).iterator();
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
        waitForCompactionToFinish();
        if (unit < 0) {
            unit = TimestampGenerator.calcUnit(numberOfSources);
        }
        return replay(e -> TimestampGenerator.checkTimestamp(sourceID, e.timestamp, unit))
                .iterator();
    }

    /**
     * Testing and debugging
     */
    protected synchronized void waitForCompactionToFinish() {
        while (compactionRunning) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while compacting");
            }
        }
    }

    private List<WALEntry> replay(Predicate<WALEntry> predicate) throws IOException {
        if (walContent == null) {
            throw new IllegalStateException("WalContent has not been loaded, cannot replay");
        }
        return walContent.stream().filter(predicate).collect(Collectors.toList());
    }

    /**
     * Also for debug & testing
     * @throws IOException
     * @return the time (ms) spent to load the WAL
     */
    protected long forceReload() throws IOException {
        long start = System.nanoTime();
        walContent = loadWAL();
        return Math.round((System.nanoTime() - start) * Math.pow(10, -6));
    }

    /**
     * Debug & testing only
     * @return
     * @throws IOException
     */
    protected List<WALEntry> loadWAL() throws IOException {
        ObjectInputStream in = null;
        List<WALEntry> entries = new ArrayList<>();

        try {
            in = new ObjectInputStream(new FileInputStream(mainWAL));

            while (true) {
                WALEntry e = (WALEntry) in.readObject();
                if (e.vote == Vote.COMMIT && e.updates != null) {
                    entries.add(e);
                }
            }
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Cannot recover from WALService: " + ex.getMessage());
        } catch (EOFException eof) {
            // does nothing
        } finally {
            if (in != null) {
                in.close();
            }
        }

        return entries;
    }

    /**
     * Credits to https://stackoverflow.com/questions/1194656/appending-to-an-objectoutputstream
     * for the solution :)
     */
    private static class NoHeaderObjectOutputStream extends ObjectOutputStream {
        public NoHeaderObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // do not write a header, but reset
            // to avoid a StreamCorruptedException on read!
            reset();
        }

    }
}
