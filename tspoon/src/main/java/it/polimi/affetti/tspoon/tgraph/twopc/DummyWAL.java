package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by affo on 31/07/17.
 */
public class DummyWAL implements WAL {
    private String fileName;
    private File wal;
    private FileWriter out;

    public DummyWAL(String fileName) {
        this.fileName = fileName;
    }

    public void open() throws IOException {
        wal = new File(fileName);
        wal.createNewFile();
        out = new FileWriter(wal, false);
    }

    public void close() throws IOException {
        out.close();
    }

    @Override
    public void addEntry(Vote vote, int timestamp, Updates updates) {
        try {
            switch (vote) {
                case REPLAY:
                    replay(timestamp);
                    break;
                case ABORT:
                    abort(timestamp);
                    break;
                default:
                    commit(timestamp, updates);
            }
        } catch (IOException e) {
            // make it crash, we cannot avoid persisting the WAL
            throw new RuntimeException("Cannot persist to WAL");
        }
    }

    @Override
    public void replay(int timestamp) throws IOException {
        out.write("R" + timestamp + ";");
        out.flush();
    }

    @Override
    public void abort(int timestamp) throws IOException {
        out.write("A" + timestamp + ";");
        out.flush();
    }

    @Override
    public void commit(int timestamp, Updates updates) throws IOException {
        out.write("C" + timestamp + "-" + updates + ";");
        out.flush();
    }
}
