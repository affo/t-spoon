package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by affo on 31/07/17.
 */
public class FileWAL {
    private String fileName;
    private final boolean overwrite;
    private File wal;
    private ObjectOutputStream out;

    public FileWAL(String fileName, boolean overwrite) {
        this.fileName = fileName;
        this.overwrite = overwrite;
    }

    public void open() throws IOException {
        wal = new File(fileName);
        wal.createNewFile();
        // if overwrite, then not append
        out = new ObjectOutputStream(new FileOutputStream(wal, !overwrite));
    }

    public void close() throws IOException {
        out.close();
    }

    public void rename(String newName) {
        File newWAL = new File(newName);
        wal.renameTo(newWAL);
        wal = newWAL;
    }

    public void addEntry(WALEntry entry) {
        try {
            out.writeObject(entry);
            out.flush();
            out.reset();
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
        if (unit < 0) {
            unit = TimestampGenerator.calcUnit(numberOfSources);
        }
        return replay(e -> TimestampGenerator.checkTimestamp(sourceID, e.timestamp, unit));
    }

    private Iterator<WALEntry> replay(Predicate<WALEntry> predicate) throws IOException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(wal));

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

    /**
     * Empties the file
     */
    public void clear() throws IOException {
        out = new ObjectOutputStream(new FileOutputStream(wal, false));
    }
}